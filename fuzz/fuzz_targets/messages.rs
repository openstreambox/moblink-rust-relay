//! Websocket protocol messages (`src/protocol.rs`) against arbitrary text.
//!
//! The relay feeds every text frame from the streamer to
//! `serde_json::from_str::<MessageToRelay>`, and the streamer does the same for
//! `MessageToStreamer`. The module is private to the crate, so it is included
//! by path rather than widening the library's public API for the harness.
#![no_main]

use libfuzzer_sys::fuzz_target;

#[allow(dead_code)]
#[path = "../../src/protocol.rs"]
mod protocol;

use protocol::*;

fuzz_target!(|data: &[u8]| {
    // The wire frames are `String`s, so non-UTF-8 never reaches the parser.
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    if let Ok(message) = serde_json::from_str::<MessageToRelay>(text) {
        // Whatever was accepted must serialize and re-parse to the same value.
        let first = serde_json::to_string(&message).expect("serializes");
        let again: MessageToRelay = serde_json::from_str(&first).expect("re-parses");
        assert_eq!(first, serde_json::to_string(&again).unwrap());

        if let MessageToRelay::Request(request) = &message {
            let response = request.to_ok_response(ResponseData::Status(StatusResponseData {
                battery_percentage: None,
            }));
            assert_eq!(response.id, request.id);
        }
    }

    if let Ok(message) = serde_json::from_str::<MessageToStreamer>(text) {
        let first = serde_json::to_string(&message).expect("serializes");
        let again: MessageToStreamer = serde_json::from_str(&first).expect("re-parses");
        assert_eq!(first, serde_json::to_string(&again).unwrap());
    }

    // Authentication is computed from peer-supplied salt and challenge.
    let mid = (0..=text.len() / 2)
        .rev()
        .find(|i| text.is_char_boundary(*i))
        .unwrap_or(0);
    let (salt, challenge) = text.split_at(mid);
    let a = calculate_authentication("password", salt, challenge);
    assert_eq!(a, calculate_authentication("password", salt, challenge));
    assert_eq!(a.len(), 44, "base64 of a SHA-256 digest");
});
