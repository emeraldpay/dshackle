// Copyright 2026 EmeraldPay Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! How consecutive log events are separated in an output stream.

use crate::config::log::LogEncoding;

/// Frame one serialized event for the output stream.
pub fn frame(encoding: &LogEncoding, event: &[u8]) -> Vec<u8> {
    match encoding {
        LogEncoding::NewLine => {
            let mut framed = Vec::with_capacity(event.len() + 1);
            framed.extend_from_slice(event);
            framed.push(b'\n');
            framed
        }
        // A 4-byte big-endian length prefix, as the legacy `LogEncodingPrefix`.
        LogEncoding::SizePrefix => {
            let mut framed = Vec::with_capacity(event.len() + 4);
            framed.extend_from_slice(&(event.len() as u32).to_be_bytes());
            framed.extend_from_slice(event);
            framed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newline_appends_single_byte() {
        assert_eq!(frame(&LogEncoding::NewLine, b"{}"), b"{}\n");
    }

    #[test]
    fn size_prefix_is_big_endian_u32() {
        assert_eq!(
            frame(&LogEncoding::SizePrefix, b"{}"),
            vec![0u8, 0, 0, 2, b'{', b'}']
        );
    }
}
