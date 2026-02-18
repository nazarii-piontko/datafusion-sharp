use anyhow::{anyhow, bail, Result};

fn first_byte(field: &'static str, bytes: &[u8]) -> Result<u8> {
    if bytes.len() != 1 {
        bail!("{field} must contain exactly one byte");
    }
    Ok(bytes[0])
}


fn opt_first_byte(field: &'static str, bytes: &Option<Vec<u8>>) -> Result<Option<u8>> {
    match bytes {
        Some(b) => Ok(Some(first_byte(field, b)?)),
        None => Ok(None)
    }
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::assigning_clones)]
pub fn from_proto_csv_options<'a>(
    pbo: Option<&'a crate::proto::CsvReadOptions>,
    schema: Option<&'a datafusion::arrow::datatypes::Schema>
) -> Result<datafusion::prelude::CsvReadOptions<'a>> {
    let Some(pbo) = pbo else {
        return Ok(datafusion::prelude::CsvReadOptions::default());
    };

    let mut dfo = datafusion::prelude::CsvReadOptions::new();

    if let Some(has_header) = pbo.has_header {
        dfo.has_header = has_header;
    }
    if let Some(delimiter) = pbo.delimiter.as_ref() && !delimiter.is_empty() {
        dfo.delimiter = first_byte("delimiter", delimiter)?;
    }
    if let Some(quote) = pbo.quote.as_ref() && !quote.is_empty() {
        dfo.quote = first_byte("quote", quote)?;
    }
    dfo.terminator = opt_first_byte("terminator", &pbo.terminator)?;
    dfo.escape = opt_first_byte("escape", &pbo.escape)?;
    dfo.comment = opt_first_byte("comment", &pbo.comment)?;
    dfo.newlines_in_values = pbo.newlines_in_values;
    dfo.schema = schema;
    if let Some(schema_infer_max_records) = pbo.schema_infer_max_records {
        dfo.schema_infer_max_records = schema_infer_max_records as usize;
    }
    if let Some(file_extension) = pbo.file_extension.as_ref() && !file_extension.is_empty() {
        dfo.file_extension = std::str::from_utf8(file_extension)?;
    }
    dfo.table_partition_cols = pbo
        .table_partition_cols
        .iter()
        .map(|c| {
            let arrow_type = c
                .arrow_type
                .as_ref()
                .ok_or_else(|| anyhow!("Partition column '{}' missing arrow type", c.name))?;

            let data_type = arrow_type.try_into()?;
            Ok((c.name.clone(), data_type))
        })
        .collect::<Result<_>>()?;
    if let Some(file_compression_type) = pbo.file_compression_type {
        dfo.file_compression_type = from_proto_file_compression(file_compression_type)?;
    }
    
    // parse sort order
    let codec = datafusion_proto::logical_plan::DefaultLogicalExtensionCodec {};
    let registry_impl = datafusion::execution::registry::MemoryFunctionRegistry::new();
    let registry: &dyn datafusion::execution::FunctionRegistry = &registry_impl;

    dfo.file_sort_order = pbo
        .file_sort_order
        .iter()
        .map(|order| {
            order
                .sort_expr_nodes
                .iter()
                .map(|node| {
                    let expr_node = node
                        .expr
                        .as_ref()
                        .ok_or_else(|| anyhow!("Missing sort expression"))?;

                    let expr = datafusion_proto::logical_plan::from_proto::parse_expr(expr_node, registry, &codec)?;

                    Ok(datafusion::logical_expr::SortExpr {
                        expr,
                        asc: node.asc,
                        nulls_first: node.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    dfo.null_regex = pbo.null_regex.as_ref()
        .map(|b| std::str::from_utf8(b).map(str::to_owned))
        .transpose()?;
    dfo.truncated_rows = pbo.truncated_rows;

    Ok(dfo)
}

use datafusion::datasource::file_format::file_compression_type::FileCompressionType as DfCompression;
use datafusion_proto::generated::datafusion_common::CompressionTypeVariant as PbCompression;

pub fn from_proto_file_compression(v: i32) -> Result<DfCompression> {
    let pb = PbCompression::try_from(v)
        .map_err(|_| anyhow!("invalid CompressionTypeVariant value: {v}"))?;

    let df = match pb {
        PbCompression::Gzip => DfCompression::GZIP,
        PbCompression::Bzip2 => DfCompression::BZIP2,
        PbCompression::Xz => DfCompression::XZ,
        PbCompression::Zstd => DfCompression::ZSTD,
        PbCompression::Uncompressed => DfCompression::UNCOMPRESSED,
    };

    Ok(df)
}
