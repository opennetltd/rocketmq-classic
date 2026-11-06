use std::io;

use rocketmq_classic_common::{compression, sysflag::message::CompressionType};

/// The compression backend snapshot returned by [`crate::Producer::compressor`].
///
/// Like Java's `Compressor`, this value exposes compression and decompression
/// operations. A value remains bound to the type it was created with if the
/// producer's compression type changes later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerCompressor {
    compression_type: CompressionType,
}

impl ProducerCompressor {
    pub(crate) const fn new(compression_type: CompressionType) -> Self {
        Self { compression_type }
    }

    /// Returns the RocketMQ compression type used by this backend.
    pub const fn compression_type(self) -> CompressionType {
        self.compression_type
    }

    /// Compresses bytes with the supplied RocketMQ compression level.
    pub fn compress(&self, source: &[u8], level: i32) -> io::Result<Vec<u8>> {
        compression::compress(self.compression_type, source, level)
    }

    /// Decompresses bytes produced by this RocketMQ compression type.
    pub fn decompress(&self, source: &[u8]) -> io::Result<Vec<u8>> {
        compression::decompress(self.compression_type, source)
    }
}

#[cfg(test)]
mod tests {
    use crate::{CompressionType, Producer, ProducerConfig};

    #[test]
    fn producer_compressor_round_trips_large_body_and_keeps_its_type_snapshot() {
        let producer = Producer::new(ProducerConfig::new("compressor-test").unwrap());
        producer.set_compress_type(CompressionType::Zlib);
        let compressor = producer.compressor();
        producer.set_compress_type(CompressionType::Zstd);

        assert_eq!(compressor.compression_type(), CompressionType::Zlib);
        assert_eq!(
            producer.compressor().compression_type(),
            CompressionType::Zstd
        );

        let body = vec![b'P'; 65 * 1024 * 1024];
        let compressed = compressor.compress(&body, 5).unwrap();
        assert_eq!(compressor.decompress(&compressed).unwrap(), body);
    }
}
