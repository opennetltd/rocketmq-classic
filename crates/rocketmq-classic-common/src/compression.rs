use std::io::{self, Read, Write};

use crate::sysflag::message::CompressionType;

/// Compress a message body using the RocketMQ wire format selected by sysFlag.
/// The LZ4 backend emits the framed format used by RocketMQ 5.5.1.
pub fn compress(compression_type: CompressionType, body: &[u8], level: i32) -> io::Result<Vec<u8>> {
    match compression_type {
        CompressionType::Zlib => {
            let level = match level {
                -1 => flate2::Compression::default(),
                0..=9 => flate2::Compression::new(level as u32),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "zlib compression level must be -1 or between 0 and 9",
                    ));
                }
            };
            let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), level);
            encoder.write_all(body)?;
            encoder.finish()
        }
        CompressionType::Lz4 => compress_lz4_frame(body),
        CompressionType::Zstd => zstd::stream::encode_all(body, level),
    }
}

/// Decompress the RocketMQ message body according to its compression type.
pub fn decompress(compression_type: CompressionType, body: &[u8]) -> io::Result<Vec<u8>> {
    decompress_limited(compression_type, body, usize::MAX)
}

/// Decompress a message body while capping the decoded bytes.
pub(crate) fn decompress_limited(
    compression_type: CompressionType,
    body: &[u8],
    max_output_bytes: usize,
) -> io::Result<Vec<u8>> {
    match compression_type {
        CompressionType::Zlib => {
            let decoder = flate2::read::ZlibDecoder::new(body);
            read_limited(decoder, max_output_bytes)
        }
        CompressionType::Lz4 => decompress_lz4_frame_limited(body, max_output_bytes),
        CompressionType::Zstd => {
            let decoder = zstd::stream::read::Decoder::new(body)?;
            read_limited(decoder, max_output_bytes)
        }
    }
}

fn read_limited(reader: impl Read, max_output_bytes: usize) -> io::Result<Vec<u8>> {
    let read_limit = u64::try_from(max_output_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut result = Vec::with_capacity(max_output_bytes.min(64 * 1024));
    reader.take(read_limit).read_to_end(&mut result)?;
    if result.len() > max_output_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed message body exceeds configured limit",
        ));
    }
    Ok(result)
}

const LZ4_FRAME_MAGIC: u32 = 0x184d_2204;
const LZ4_MAX_BLOCK: usize = 4 * 1024 * 1024;

fn compress_lz4_frame(body: &[u8]) -> io::Result<Vec<u8>> {
    let descriptor = [0x60, 0x70]; // v1, independent blocks, 4 MiB maximum block.
    let header_checksum = (xxhash_rust::xxh32::xxh32(&descriptor, 0) >> 8) as u8;
    let mut result = Vec::with_capacity(body.len() + 16);
    result.extend_from_slice(&LZ4_FRAME_MAGIC.to_le_bytes());
    result.extend_from_slice(&descriptor);
    result.push(header_checksum);

    for block in body.chunks(LZ4_MAX_BLOCK) {
        let compressed = lz4_flex::block::compress(block);
        let (encoded, uncompressed) = if compressed.len() >= block.len() {
            (block, true)
        } else {
            (compressed.as_slice(), false)
        };
        let size = u32::try_from(encoded.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "LZ4 block exceeds frame length",
            )
        })?;
        let tagged_size = if uncompressed { size | (1 << 31) } else { size };
        result.extend_from_slice(&tagged_size.to_le_bytes());
        result.extend_from_slice(encoded);
    }
    result.extend_from_slice(&0_u32.to_le_bytes());
    Ok(result)
}

fn decompress_lz4_frame_limited(frame: &[u8], max_output_bytes: usize) -> io::Result<Vec<u8>> {
    fn invalid_frame() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, "invalid LZ4 frame")
    }
    fn take<'a>(frame: &'a [u8], cursor: &mut usize, count: usize) -> io::Result<&'a [u8]> {
        let end = cursor.checked_add(count).ok_or_else(invalid_frame)?;
        let bytes = frame.get(*cursor..end).ok_or_else(invalid_frame)?;
        *cursor = end;
        Ok(bytes)
    }

    let mut cursor = 0;
    let mut result = Vec::new();
    let mut saw_frame = false;

    while cursor < frame.len() {
        let magic = u32::from_le_bytes(
            take(frame, &mut cursor, 4)?
                .try_into()
                .map_err(|_| invalid_frame())?,
        );
        if (0x184d_2a50..=0x184d_2a5f).contains(&magic) {
            let skip_length = u32::from_le_bytes(
                take(frame, &mut cursor, 4)?
                    .try_into()
                    .map_err(|_| invalid_frame())?,
            );
            let skip_length = usize::try_from(skip_length).map_err(|_| invalid_frame())?;
            take(frame, &mut cursor, skip_length)?;
            saw_frame = true;
            continue;
        }
        if magic != LZ4_FRAME_MAGIC {
            return Err(invalid_frame());
        }

        let frame_output_start = result.len();
        let flg = take(frame, &mut cursor, 1)?[0];
        let bd = take(frame, &mut cursor, 1)?[0];
        if flg >> 6 != 1 || flg & 0x20 == 0 || flg & 0x02 != 0 || bd & 0x8f != 0 {
            return Err(invalid_frame());
        }
        let max_block = match (bd >> 4) & 7 {
            4 => 64 * 1024,
            5 => 256 * 1024,
            6 => 1024 * 1024,
            7 => LZ4_MAX_BLOCK,
            _ => return Err(invalid_frame()),
        };
        let mut descriptor = vec![flg, bd];
        let expected_content_size = if flg & 0x08 != 0 {
            let bytes = take(frame, &mut cursor, 8)?;
            descriptor.extend_from_slice(bytes);
            Some(u64::from_le_bytes(
                bytes.try_into().map_err(|_| invalid_frame())?,
            ))
        } else {
            None
        };
        if flg & 0x01 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "LZ4 dictionaries are unsupported",
            ));
        }
        let header_checksum = take(frame, &mut cursor, 1)?[0];
        if header_checksum != (xxhash_rust::xxh32::xxh32(&descriptor, 0) >> 8) as u8 {
            return Err(invalid_frame());
        }

        let block_checksum = flg & 0x10 != 0;
        let content_checksum = flg & 0x04 != 0;
        loop {
            let tagged = u32::from_le_bytes(
                take(frame, &mut cursor, 4)?
                    .try_into()
                    .map_err(|_| invalid_frame())?,
            );
            if tagged == 0 {
                break;
            }
            let raw = tagged & (1 << 31) != 0;
            let size = (tagged & !(1 << 31)) as usize;
            if size == 0 || size > max_block {
                return Err(invalid_frame());
            }
            let encoded = take(frame, &mut cursor, size)?;
            if raw {
                let new_len = result.len().checked_add(size).ok_or_else(invalid_frame)?;
                if new_len > max_output_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "decompressed message body exceeds configured limit",
                    ));
                }
                result.extend_from_slice(encoded);
            } else {
                let remaining = max_output_bytes.saturating_sub(result.len());
                let mut decoded = vec![0; max_block.min(remaining.saturating_add(1))];
                let written = match lz4_flex::block::decompress_into(encoded, &mut decoded) {
                    Ok(written) => written,
                    Err(lz4_flex::block::DecompressError::OutputTooSmall { expected, .. })
                        if expected > remaining =>
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "decompressed message body exceeds configured limit",
                        ));
                    }
                    Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
                };
                let new_len = result
                    .len()
                    .checked_add(written)
                    .ok_or_else(invalid_frame)?;
                if new_len > max_output_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "decompressed message body exceeds configured limit",
                    ));
                }
                result.extend_from_slice(&decoded[..written]);
            }
            if block_checksum {
                let checksum = u32::from_le_bytes(
                    take(frame, &mut cursor, 4)?
                        .try_into()
                        .map_err(|_| invalid_frame())?,
                );
                if checksum != xxhash_rust::xxh32::xxh32(encoded, 0) {
                    return Err(invalid_frame());
                }
            }
        }
        if content_checksum {
            let checksum = u32::from_le_bytes(
                take(frame, &mut cursor, 4)?
                    .try_into()
                    .map_err(|_| invalid_frame())?,
            );
            if checksum != xxhash_rust::xxh32::xxh32(&result[frame_output_start..], 0) {
                return Err(invalid_frame());
            }
        }
        if expected_content_size.is_some_and(|size| {
            size != u64::try_from(result.len() - frame_output_start).unwrap_or(u64::MAX)
        }) {
            return Err(invalid_frame());
        }
        saw_frame = true;
    }

    if !saw_frame {
        return Err(invalid_frame());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_java_compression_type_round_trips_message_bytes() {
        let body = vec![b'R'; 8_192];
        for kind in CompressionType::values() {
            let compressed = compress(kind, &body, 5).unwrap();
            assert_eq!(decompress(kind, &compressed).unwrap(), body, "{kind}");
        }
    }

    #[test]
    fn public_decompression_round_trips_bodies_larger_than_64_mib() {
        let body = vec![b'R'; 65 * 1024 * 1024];
        for kind in CompressionType::values() {
            let compressed = compress(kind, &body, 5).unwrap();
            let decompressed = decompress(kind, &compressed).unwrap();
            assert_eq!(decompressed, body, "{kind}");
        }
    }

    #[test]
    fn every_public_decompressor_rejects_malformed_input() {
        for kind in CompressionType::values() {
            assert!(
                decompress(kind, b"not a valid compressed message").is_err(),
                "{kind}"
            );
        }
    }

    #[test]
    fn zlib_preserves_java_level_validation() {
        assert_eq!(
            compress(CompressionType::Zlib, b"body", 10)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(compress(CompressionType::Zlib, b"body", -1).is_ok());
    }

    #[test]
    fn every_java_compression_type_stops_at_the_internal_output_cap() {
        let input = vec![b'X'; 32 * 1024];
        for codec in CompressionType::values() {
            let encoded = compress(codec, &input, 3).unwrap();
            assert!(decompress_limited(codec, &encoded, 128).is_err(), "{codec}");
        }
    }

    fn skippable_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&0x184d_2a53_u32.to_le_bytes());
        frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn checksummed_lz4_frame(body: &[u8]) -> Vec<u8> {
        let descriptor = [0x74, 0x70]; // independent blocks, block and content checksums.
        let mut frame = Vec::new();
        frame.extend_from_slice(&LZ4_FRAME_MAGIC.to_le_bytes());
        frame.extend_from_slice(&descriptor);
        frame.push((xxhash_rust::xxh32::xxh32(&descriptor, 0) >> 8) as u8);
        let tagged_size = u32::try_from(body.len()).unwrap() | (1 << 31);
        frame.extend_from_slice(&tagged_size.to_le_bytes());
        frame.extend_from_slice(body);
        frame.extend_from_slice(&xxhash_rust::xxh32::xxh32(body, 0).to_le_bytes());
        frame.extend_from_slice(&0_u32.to_le_bytes());
        frame.extend_from_slice(&xxhash_rust::xxh32::xxh32(body, 0).to_le_bytes());
        frame
    }

    #[test]
    fn lz4_decompression_reads_concatenated_frames_and_skips_skippable_frames() {
        let first = compress_lz4_frame(b"first-").unwrap();
        let second = compress_lz4_frame(b"second").unwrap();
        let mut concatenated = first.clone();
        concatenated.extend_from_slice(&second);
        assert_eq!(
            decompress(CompressionType::Lz4, &concatenated).unwrap(),
            b"first-second"
        );

        let mut with_skippable_prefix = skippable_frame(b"ignored");
        with_skippable_prefix.extend_from_slice(&first);
        let mut with_skippable_middle = first;
        with_skippable_middle.extend_from_slice(&skippable_frame(b"also ignored"));
        with_skippable_middle.extend_from_slice(&second);
        assert_eq!(
            decompress(CompressionType::Lz4, &with_skippable_prefix).unwrap(),
            b"first-"
        );
        assert_eq!(
            decompress(CompressionType::Lz4, &with_skippable_middle).unwrap(),
            b"first-second"
        );
        assert_eq!(
            decompress(CompressionType::Lz4, &compress_lz4_frame(b"").unwrap()).unwrap(),
            b""
        );
        assert_eq!(
            decompress(CompressionType::Lz4, &skippable_frame(b"only ignored")).unwrap(),
            b""
        );
    }

    #[test]
    fn lz4_internal_output_limit_is_cumulative_across_frames() {
        let mut stream = compress_lz4_frame(&[b'A'; 64]).unwrap();
        stream.extend_from_slice(&compress_lz4_frame(&[b'B'; 64]).unwrap());
        assert!(decompress_lz4_frame_limited(&stream, 100).is_err());
        assert_eq!(
            decompress_lz4_frame_limited(&stream, 128).unwrap().len(),
            128
        );
    }

    #[test]
    fn lz4_stream_rejects_truncated_frames_bad_checksums_and_invalid_tails() {
        let valid = compress_lz4_frame(b"valid").unwrap();
        assert!(decompress(CompressionType::Lz4, &valid[..valid.len() - 1]).is_err());

        let mut truncated_skip = valid.clone();
        truncated_skip.extend_from_slice(&0x184d_2a50_u32.to_le_bytes());
        truncated_skip.extend_from_slice(&3_u32.to_le_bytes());
        truncated_skip.push(b'x');
        assert!(decompress(CompressionType::Lz4, &truncated_skip).is_err());

        let mut bad_checksum = checksummed_lz4_frame(b"checksum");
        let block_checksum_position = 7 + 4 + b"checksum".len();
        bad_checksum[block_checksum_position] ^= 1;
        assert!(decompress(CompressionType::Lz4, &bad_checksum).is_err());

        let mut invalid_tail = valid;
        invalid_tail.extend_from_slice(&0x0102_0304_u32.to_le_bytes());
        assert!(decompress(CompressionType::Lz4, &invalid_tail).is_err());
    }
}
