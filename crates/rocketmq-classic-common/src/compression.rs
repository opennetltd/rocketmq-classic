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
    let magic = u32::from_le_bytes(
        take(frame, &mut cursor, 4)?
            .try_into()
            .map_err(|_| invalid_frame())?,
    );
    if magic != LZ4_FRAME_MAGIC {
        return Err(invalid_frame());
    }
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
    let mut result = Vec::new();
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
        if size > max_block {
            return Err(invalid_frame());
        }
        let encoded = take(frame, &mut cursor, size)?;
        let old_len = result.len();
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
        if result.len() < old_len {
            return Err(invalid_frame());
        }
    }
    if content_checksum {
        let checksum = u32::from_le_bytes(
            take(frame, &mut cursor, 4)?
                .try_into()
                .map_err(|_| invalid_frame())?,
        );
        if checksum != xxhash_rust::xxh32::xxh32(&result, 0) {
            return Err(invalid_frame());
        }
    }
    if cursor != frame.len()
        || expected_content_size.is_some_and(|size| size != result.len() as u64)
    {
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
}
