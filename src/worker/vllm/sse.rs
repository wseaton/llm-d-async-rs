//! Server-sent events decoding: the `data` of each event, from bytes that
//! arrive split at arbitrary points. Comments, `event`, `id` and `retry`
//! fields are ignored; an event left open when the stream ends is dropped.

#[derive(Debug, thiserror::Error)]
#[error("event stream line is not UTF-8")]
pub struct NotUtf8;

#[derive(Debug, Default)]
pub struct SseDecoder {
    /// Bytes after the last complete line.
    pending: Vec<u8>,
    data: String,
    has_data: bool,
}

impl SseDecoder {
    /// Appends the data of every event `chunk` completes to `events`.
    pub fn feed(&mut self, chunk: &[u8], events: &mut Vec<String>) -> Result<(), NotUtf8> {
        self.pending.extend_from_slice(chunk);
        let mut start = 0;
        while let Some(len) = self.pending[start..].iter().position(|&b| b == b'\n') {
            let end = start + len;
            let line = self.pending[start..end]
                .strip_suffix(b"\r")
                .unwrap_or(&self.pending[start..end]);
            let line = std::str::from_utf8(line).map_err(|_| NotUtf8)?;
            if line.is_empty() {
                if self.has_data {
                    events.push(std::mem::take(&mut self.data));
                    self.has_data = false;
                }
            } else if let Some(value) = field_value(line, "data") {
                if self.has_data {
                    self.data.push('\n');
                }
                self.data.push_str(value);
                self.has_data = true;
            }
            start = end + 1;
        }
        self.pending.drain(..start);
        Ok(())
    }
}

/// The value of `line` when it is the field `name`, without the one
/// optional space after the colon.
fn field_value<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(name)?;
    if rest.is_empty() {
        return Some("");
    }
    let value = rest.strip_prefix(':')?;
    Some(value.strip_prefix(' ').unwrap_or(value))
}

#[cfg(test)]
mod tests {
    use crate::worker::vllm::sse::SseDecoder;

    fn decode_in_pieces(input: &[u8], piece: usize) -> Vec<String> {
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        for chunk in input.chunks(piece) {
            decoder.feed(chunk, &mut events).unwrap();
        }
        events
    }

    #[test]
    fn events_survive_any_split() {
        let input = "data: {\"a\":\"é😀\"}\n\n: keep-alive\n\ndata: [DONE]\n\n".as_bytes();
        for piece in 1..=input.len() {
            assert_eq!(
                decode_in_pieces(input, piece),
                ["{\"a\":\"é😀\"}", "[DONE]"],
                "piece {piece}"
            );
        }
    }

    #[test]
    fn crlf_and_multiline_data() {
        let input = b"event: x\r\ndata: one\r\ndata:two\r\nid: 7\r\n\r\ndata\r\n\r\n";
        assert_eq!(decode_in_pieces(input, 3), ["one\ntwo", ""]);
    }

    #[test]
    fn fields_are_matched_whole() {
        assert_eq!(decode_in_pieces(b"database: x\n\ndata: y\n\n", 4), ["y"]);
    }

    #[test]
    fn unterminated_event_is_dropped() {
        assert_eq!(decode_in_pieces(b"data: a\n\ndata: b\n", 2), ["a"]);
        assert_eq!(decode_in_pieces(b"data: a\n\ndata: b", 2), ["a"]);
    }

    #[test]
    fn a_blank_line_without_data_is_no_event() {
        assert!(decode_in_pieces(b"\n\n: c\n\n", 1).is_empty());
    }

    #[test]
    fn invalid_utf8_is_an_error() {
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        assert!(decoder.feed(b"data: \xff\n", &mut events).is_err());
    }
}
