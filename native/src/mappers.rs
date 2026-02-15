use anyhow::{anyhow, Context, Result};

fn first_byte(field: &'static str, str: &String) -> Result<u8> {
    let bytes = str.as_bytes();
    if bytes.len() != 1 {
        anyhow::bail!("{field} must contain exactly one byte");
    }

    let b = bytes
        .first()
        .copied()
        .with_context(|| format!("{field} must contain exactly one byte (got empty)"))?;
    Ok(b)
}


fn opt_first_byte(field: &'static str, str: Option<&String>) -> Result<Option<u8>> {
    match str {
        Some(b) => Ok(Some(first_byte(field, b)?)),
        None => Ok(None)
    }
}

pub fn from_proto_csv_options<'a>(
    pbo: &'a crate::proto::CsvReadOptions,
    schema: Option<&'a datafusion::arrow::datatypes::Schema>
) -> Result<datafusion::prelude::CsvReadOptions<'a>> {
    let mut dfo = datafusion::prelude::CsvReadOptions::new();

    dfo.has_header = pbo.has_header;

    dfo.delimiter = first_byte("delimiter", &pbo.delimiter)?;
    dfo.quote = first_byte("quote", &pbo.quote)?;

    dfo.terminator = opt_first_byte("terminator", pbo.terminator.as_ref())?;
    dfo.escape = opt_first_byte("escape", pbo.escape.as_ref())?;
    dfo.comment = opt_first_byte("comment", pbo.comment.as_ref())?;

    dfo.newlines_in_values = pbo.newlines_in_values;
    dfo.schema = schema;
    dfo.schema_infer_max_records = pbo.schema_infer_max_records as usize;
    dfo.file_extension = &pbo.file_extension;

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

    dfo.file_compression_type = from_proto_file_compression(pbo.file_compression_type)?;

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

    dfo.null_regex = pbo.null_regex.clone();
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
