//! Planning and sending resumable requests through vLLM's token layer: the
//! queue's render server renders the caller's request, the gateway streams
//! `/inference/v1/generate`, and the render server derenders the output.

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName};

use crate::api::progress::Progress;
use crate::dispatch::message::ResumeTarget;
use crate::worker::client::{
    ClientError, ErrorCategory, InferenceClient, InferenceResponse, ResponseBody, stream_failed,
};
use crate::worker::vllm::{self, Api, Plan, StreamError, derender};

const GENERATE_PATH: &str = "/inference/v1/generate";
const EPP_PROFILE: HeaderName = HeaderName::from_static("epp-profile");

/// A resumable queue's settings.
#[derive(Debug, Clone, Copy)]
pub struct Resuming<'a> {
    pub igw_base_url: &'a str,
    pub target: &'a ResumeTarget,
}

fn join(base: &str, path: &str) -> String {
    format!("{}{path}", base.trim_end_matches('/'))
}

fn unusable(what: &str, e: impl std::fmt::Display) -> Box<ClientError> {
    Box::new(ClientError::new(
        ErrorCategory::Unknown,
        format!("{what}: {e}"),
    ))
}

impl Resuming<'_> {
    /// How to send `body`, the inline payload if the request has one, to
    /// `url` through the token layer, continuing `progress` when it can be
    /// continued and clearing it when not. `None` sends the request as
    /// submitted. Fails, keeping `progress`, only when rendering does.
    pub async fn plan(
        &self,
        client: &InferenceClient,
        url: &str,
        headers: &HeaderMap,
        body: Option<&Bytes>,
        progress: &mut Option<Progress>,
    ) -> Result<Option<Plan>, Box<ClientError>> {
        let planned = self
            .plan_inner(client, url, headers, body, progress.as_ref())
            .await?;
        if !planned.as_ref().is_some_and(|p| p.resumed) {
            *progress = None;
        }
        Ok(planned)
    }

    async fn plan_inner(
        &self,
        client: &InferenceClient,
        url: &str,
        headers: &HeaderMap,
        body: Option<&Bytes>,
        progress: Option<&Progress>,
    ) -> Result<Option<Plan>, Box<ClientError>> {
        let (Some(body), Some(api)) = (body, Api::from_url(url)) else {
            return Ok(None);
        };
        let caller = match vllm::check(api, body) {
            Ok(caller) => caller,
            Err(reason) => {
                tracing::debug!(%reason, "sending as submitted");
                return Ok(None);
            }
        };
        if let Some(plan) = progress.and_then(|p| Plan::finished(caller, p)) {
            return Ok(Some(plan));
        }
        let rendered = client
            .post_json(
                &join(&self.target.render_url, api.render_path()),
                headers.clone(),
                body.to_vec(),
            )
            .await?;
        let planned =
            Plan::generate(caller, &rendered, progress).map_err(|e| unusable("render", e))?;
        Ok(Some(match planned {
            Ok(plan) => plan,
            Err(reason) => {
                tracing::info!(%reason, "restarting the generation");
                Plan::generate(caller, &rendered, None)
                    .map_err(|e| unusable("render", e))?
                    .map_err(|e| unusable("render", e))?
            }
        }))
    }

    /// Sends `plan` and returns the response the caller's non-streamed
    /// request `payload` would have got. On failure `plan` keeps the output
    /// so far, finished or not.
    pub async fn send(
        &self,
        client: &InferenceClient,
        headers: HeaderMap,
        plan: &mut Plan,
        payload: &Bytes,
    ) -> Result<InferenceResponse, Box<ClientError>> {
        let mut status = 200;
        if let Some(body) = plan.body.clone() {
            let mut generate_headers = headers.clone();
            if let Some(profile) = &self.target.generate_epp_profile {
                generate_headers.insert(EPP_PROFILE, profile.header_value().clone());
            }
            status = client
                .send_streamed(
                    &join(self.igw_base_url, GENERATE_PATH),
                    generate_headers,
                    body,
                    &mut plan.reassembly,
                )
                .await?;
        }
        let generated = plan
            .reassembly
            .finish()
            .map_err(|e| Box::new(stream_failed(e)))?;
        let api = plan.caller.api;
        let id = derender::response_id(api);
        let request = derender::request(api, &generated, &id, payload)
            .map_err(|e| Box::new(stream_failed(StreamError::Malformed(e))))?;
        let derendered = client
            .post_json(
                &join(&self.target.render_url, api.derender_path()),
                headers,
                request,
            )
            .await?;
        let json = derender::response(plan.caller, &derendered, generated)
            .map_err(|e| unusable("derender", e))?;
        Ok(InferenceResponse {
            status,
            body: ResponseBody::Inline(json),
        })
    }
}
