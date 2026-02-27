use anyhow::{anyhow, bail, Result};

use datafusion::arrow::datatypes::{DataType, Schema};

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::CsvReadOptions;
use crate::proto;

pub(crate) fn from_proto_schema(schema: Option<&datafusion_proto::protobuf::Schema>) -> Result<Option<Schema>> {
    schema
        .map(TryFrom::try_from)
        .transpose()
        .map_err(|e| anyhow!("Failed to parse schema from options: {e}"))
}

#[warn(clippy::field_reassign_with_default)]
pub(crate) fn from_proto_csv_options<'a>(
    pbo: Option<&'a proto::CsvReadOptions>,
    schema: Option<&'a Schema>
) -> Result<CsvReadOptions<'a>> {
    let mut dfo = CsvReadOptions::default();
    let Some(pbo) = pbo else { return Ok(dfo) };

    if let Some(has_header) = pbo.has_header {
        dfo.has_header = has_header;
    }
    if let Some(delimiter) = pbo.delimiter.as_ref() && !delimiter.is_empty() {
        dfo.delimiter = first_byte("delimiter", delimiter)?;
    }
    if let Some(quote) = pbo.quote.as_ref() && !quote.is_empty() {
        dfo.quote = first_byte("quote", quote)?;
    }
    dfo.terminator = opt_first_byte("terminator", pbo.terminator.as_ref())?;
    dfo.escape = opt_first_byte("escape", pbo.escape.as_ref())?;
    dfo.comment = opt_first_byte("comment", pbo.comment.as_ref())?;
    if let Some(newlines_in_values) = pbo.newlines_in_values {
        dfo.newlines_in_values = newlines_in_values;
    }
    dfo.schema = schema;
    if let Some(schema_infer_max_records) = pbo.schema_infer_max_records {
        dfo.schema_infer_max_records = usize::try_from(schema_infer_max_records)?;
    }
    if let Some(file_extension) = pbo.file_extension.as_ref() && !file_extension.is_empty() {
        dfo.file_extension = std::str::from_utf8(file_extension)?;
    }
    dfo.table_partition_cols = from_proto_table_partition_cols(&pbo.table_partition_cols)?;
    if let Some(file_compression_type) = pbo.file_compression_type {
        dfo.file_compression_type = from_proto_file_compression(file_compression_type)?;
    }
    dfo.file_sort_order = from_proto_file_sort_order(&pbo.file_sort_order)?;
    dfo.null_regex = pbo.null_regex.as_ref().map(|b| std::str::from_utf8(b).map(str::to_owned)).transpose()?;
    if let Some(truncated_rows) = pbo.truncated_rows {
        dfo.truncated_rows = truncated_rows;
    }

    Ok(dfo)
}

#[warn(clippy::field_reassign_with_default)]
pub(crate) fn from_proto_json_read_options<'a>(
    pbo: Option<&'a proto::JsonReadOptions>,
    schema: Option<&'a Schema>
) -> Result<datafusion::prelude::NdJsonReadOptions<'a>> {
    let mut dfo = datafusion::prelude::NdJsonReadOptions::default();
    let Some(pbo) = pbo else { return Ok(dfo) };

    dfo.schema = schema;
    if let Some(schema_infer_max_records) = pbo.schema_infer_max_records {
        dfo.schema_infer_max_records = usize::try_from(schema_infer_max_records)?;
    }
    if let Some(file_extension) = pbo.file_extension.as_ref() && !file_extension.is_empty() {
        dfo.file_extension = std::str::from_utf8(file_extension)?;
    }
    dfo.table_partition_cols = from_proto_table_partition_cols(&pbo.table_partition_cols)?;
    if let Some(file_compression_type) = pbo.file_compression_type {
        dfo.file_compression_type = from_proto_file_compression(file_compression_type)?;
    }
    dfo.file_sort_order = from_proto_file_sort_order(&pbo.file_sort_order)?;

    Ok(dfo)
}

#[warn(clippy::field_reassign_with_default)]
pub(crate) fn from_proto_dataframe_write_options(pbo: Option<&proto::DataFrameWriteOptions>) -> Result<datafusion::dataframe::DataFrameWriteOptions> {
    let dfo = datafusion::dataframe::DataFrameWriteOptions::default();
    let Some(pbo) = pbo else { return Ok(dfo) };

    let mut dfo = dfo
        .with_insert_operation(from_proto_insert_op(pbo.insert_op)?)
        .with_single_file_output(pbo.single_file_output)
        .with_partition_by(pbo.partition_by.clone());

    if let Some(sort_by) = pbo.sort_by.clone() {
        dfo = dfo.with_sort_by(from_proto_file_sort_order(&[sort_by])?.first().ok_or(anyhow!("Invalid sort by"))?.clone());
    }

    Ok(dfo)
}

fn first_byte(field: &'static str, bytes: &[u8]) -> Result<u8> {
    match bytes {
        [b] => Ok(*b),
        _ => bail!("{field} must contain exactly one byte"),
    }
}

fn opt_first_byte(field: &'static str, bytes: Option<&Vec<u8>>) -> Result<Option<u8>> {
    bytes.map(|b| first_byte(field, b)).transpose()
}

fn from_proto_table_partition_cols(table_partition_cols: &[datafusion_proto::protobuf::PartitionColumn]) -> Result<Vec<(String, DataType)>> {
    table_partition_cols.iter()
        .map(|c| {
            let arrow_type = c
                .arrow_type
                .as_ref()
                .ok_or_else(|| anyhow!("Partition column '{}' missing arrow type", c.name))?;

            let data_type = arrow_type.try_into()?;
            Ok((c.name.clone(), data_type))
        })
        .collect::<Result<_>>()
}

fn from_proto_file_sort_order(file_sort_order: &[datafusion_proto::protobuf::SortExprNodeCollection]) -> Result<Vec<Vec<SortExpr>>> {
    let codec = datafusion_proto::logical_plan::DefaultLogicalExtensionCodec {};
    let registry_impl = datafusion::execution::registry::MemoryFunctionRegistry::new();
    let registry: &dyn datafusion::execution::FunctionRegistry = &registry_impl;

    file_sort_order.iter()
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

                    Ok(SortExpr {
                        expr,
                        asc: node.asc,
                        nulls_first: node.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()
}

fn from_proto_file_compression(v: i32) -> Result<FileCompressionType> {
    use datafusion_proto::generated::datafusion_common::CompressionTypeVariant as PbCompression;

    let pb = PbCompression::try_from(v)
        .map_err(|_| anyhow!("invalid CompressionTypeVariant value: {v}"))?;

    let df = match pb {
        PbCompression::Gzip => FileCompressionType::GZIP,
        PbCompression::Bzip2 => FileCompressionType::BZIP2,
        PbCompression::Xz => FileCompressionType::XZ,
        PbCompression::Zstd => FileCompressionType::ZSTD,
        PbCompression::Uncompressed => FileCompressionType::UNCOMPRESSED,
    };

    Ok(df)
}

fn from_proto_insert_op(v: i32) -> Result<datafusion::logical_expr::dml::InsertOp> {
    use datafusion_proto::generated::datafusion::InsertOp as PbInsertOp;
    use datafusion::logical_expr::dml::InsertOp;

    let pb = PbInsertOp::try_from(v)
        .map_err(|_| anyhow!("invalid InsertOp value: {v}"))?;

    let df = match pb {
        PbInsertOp::Append => InsertOp::Append,
        PbInsertOp::Overwrite => InsertOp::Overwrite,
        PbInsertOp::Replace => InsertOp::Replace,
    };

    Ok(df)
}