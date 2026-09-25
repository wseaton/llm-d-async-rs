//! Planning and sending resumable requests: a first attempt streams the
//! caller's request; a completion continues from its saved tokens on the
//! same endpoint; a chat request continues on `/inference/v1/generate` from
//! the prompt vLLM renders for it, and vLLM derenders the whole output.

use bytes::Bytes;
use reqwest::header::HeaderMap;

use crate::api::progress::Progress;
use crate::clock::now_millis;
use crate::worker::client::{
    ClientError, ErrorCategory, InferenceClient, InferenceResponse, ResponseBody, stream_failed,
};
use crate::worker::vllm::{self, Api, Finished, Plan, Target, ToolCallParser};

const RENDER_PATH: &str = "/v1/chat/completions/render";
const DERENDER_PATH: &str = "/v1/chat/completions/derender";
const GENERATE_PATH: &str = "/inference/v1/generate";

/// A resumable queue's settings.
#[derive(Debug, Clone, Copy)]
pub struct Resuming<'a> {
    pub igw_base_url: &'a str,
    pub tool_call_parser: Option<ToolCallParser>,
    pub render_url: Option<&'a str>,
}

fn join(base: &str, path: &str) -> String {
    format!("{}{path}", base.trim_end_matches('/'))
}

impl Resuming<'_> {
    /// How to stream `body`, the inline payload if the request has one, to
    /// `url`, continuing `progress` when it can be continued and clearing it
    /// when not. `None` sends the request as submitted. Fails, keeping
    /// `progress`, only when rendering does.
    pub async fn plan(
        &self,
        client: &InferenceClient,
        url: &str,
        headers: &HeaderMap,
        body: Option<&Bytes>,
        progress: &mut Option<Progress>,
    ) -> Result<Option<Plan>, Box<ClientError>> {
        let planned = self
            .plan_inner(client, url, headers, body, progress)
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
        progress: &Option<Progress>,
    ) -> Result<Option<Plan>, Box<ClientError>> {
        let Some(body) = body else {
            return Ok(None);
        };
        let Some(api) = Api::from_url(url) else {
            return Ok(None);
        };
        if api == Api::Chat
            && let (Some(saved), Some(render_url)) = (progress, self.render_url)
        {
            let rendered = client
                .post_json(
                    &join(render_url, RENDER_PATH),
                    headers.clone(),
                    body.to_vec(),
                )
                .await?;
            match vllm::continue_chat(&rendered, saved, body) {
                Ok(plan) => return Ok(Some(plan)),
                Err(reason) => tracing::info!(%reason, "restarting the chat generation"),
            }
        }
        Ok(vllm::plan(
            api,
            self.tool_call_parser,
            self.render_url.is_some(),
            body,
            progress.as_ref(),
        )
        .inspect_err(|reason| tracing::debug!(%reason, "sending without streaming"))
        .ok())
    }

    /// Sends `plan` and returns the response the caller's non-streamed
    /// request would have got. `chat_request` is the caller's body, which a
    /// continued chat generation is derendered against.
    pub async fn send(
        &self,
        client: &InferenceClient,
        url: &str,
        headers: HeaderMap,
        plan: &mut Plan,
        chat_request: &Bytes,
    ) -> Result<InferenceResponse, Box<ClientError>> {
        let target = match plan.target {
            Target::Request => url.to_owned(),
            Target::Generate => join(self.igw_base_url, GENERATE_PATH),
        };
        let (status, finished) = client
            .send_streamed(
                &target,
                headers.clone(),
                plan.body.clone(),
                &mut plan.reassembly,
            )
            .await?;
        let json = match finished {
            Finished::Response(json) => json,
            Finished::Generated {
                generated,
                caller_token_ids,
            } => {
                let render_url = self.render_url.ok_or_else(|| {
                    Box::new(ClientError::new(
                        ErrorCategory::Unknown,
                        "a continued chat generation needs the queue's render_url",
                    ))
                })?;
                let id = format!("chatcmpl-{:016x}", rand::random::<u64>());
                let request = vllm::chat::derender_request(&generated, &id, chat_request)
                    .map_err(|e| Box::new(stream_failed(e.into())))?;
                let derendered = client
                    .post_json(&join(render_url, DERENDER_PATH), headers, request)
                    .await?;
                let created = u64::try_from(now_millis() / 1000).unwrap_or(0);
                let response = vllm::chat::resumed_response(
                    &derendered,
                    id,
                    created,
                    generated,
                    caller_token_ids,
                )
                .map_err(|e| Box::new(stream_failed(e)))?;
                serde_json::to_string(&response).map_err(|e| Box::new(stream_failed(e.into())))?
            }
        };
        Ok(InferenceResponse {
            status,
            body: ResponseBody::Inline(json),
        })
    }
}
