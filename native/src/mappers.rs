use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

use crate::wire::data_fusion_sharp::formats::FileCompressionType as WireFileCompressionType;

impl WireFileCompressionType {
    pub fn to_datafusion(self) -> FileCompressionType {
        let t = match self {
            WireFileCompressionType::GZip => CompressionTypeVariant::GZIP,
            WireFileCompressionType::BZip2 => CompressionTypeVariant::BZIP2,
            WireFileCompressionType::Xz => CompressionTypeVariant::XZ,
            WireFileCompressionType::ZStd => CompressionTypeVariant::ZSTD,
            WireFileCompressionType::Uncompressed => CompressionTypeVariant::UNCOMPRESSED,
            _ => CompressionTypeVariant::UNCOMPRESSED,
        };

        t.into()
    }
}
