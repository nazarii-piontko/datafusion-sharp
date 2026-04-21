use anyhow::{Result, anyhow, bail};
use std::collections::HashMap;

use crate::data_frame_param_values::Values;
use crate::proto;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::ParamValues;
use datafusion::common::metadata::{FieldMetadata, ScalarAndMetadata};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::CsvReadOptions;

pub(crate) fn from_proto_schema(
    schema: Option<&datafusion_proto::protobuf::Schema>,
) -> Result<Option<Schema>> {
    schema
        .map(TryFrom::try_from)
        .transpose()
        .map_err(|e| anyhow!("Failed to parse schema from options: {e}"))
}

#[warn(clippy::field_reassign_with_default)]
pub(crate) fn from_proto_csv_options<'a>(
    pbo: Option<&'a proto::CsvReadOptions>,
    schema: Option<&'a Schema>,
) -> Result<CsvReadOptions<'a>> {
    let mut dfo = CsvReadOptions::default();
    let Some(pbo) = pbo else { return Ok(dfo) };

    if let Some(has_header) = pbo.has_header {
        dfo.has_header = has_header;
    }
    if let Some(delimiter) = pbo.delimiter.as_ref()
        && !delimiter.is_empty()
    {
        dfo.delimiter = first_byte("delimiter", delimiter)?;
    }
    if let Some(quote) = pbo.quote.as_ref()
        && !quote.is_empty()
    {
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
    if let Some(file_extension) = pbo.file_extension.as_ref()
        && !file_extension.is_empty()
    {
        dfo.file_extension = std::str::from_utf8(file_extension)?;
    }
    dfo.table_partition_cols = from_proto_table_partition_cols(&pbo.table_partition_cols)?;
    if let Some(file_compression_type) = pbo.file_compression_type {
        dfo.file_compression_type = from_proto_file_compression(file_compression_type)?;
    }
    dfo.file_sort_order = from_proto_file_sort_order(&pbo.file_sort_order)?;
    dfo.null_regex = pbo
        .null_regex
        .as_ref()
        .map(|b| std::str::from_utf8(b).map(str::to_owned))
        .transpose()?;
    if let Some(truncated_rows) = pbo.truncated_rows {
        dfo.truncated_rows = truncated_rows;
    }

    Ok(dfo)
}

#[warn(clippy::field_reassign_with_default)]
pub(crate) fn from_proto_json_read_options<'a>(
    pbo: Option<&'a proto::JsonReadOptions>,
    schema: Option<&'a Schema>,
) -> Result<datafusion::prelude::JsonReadOptions<'a>> {
    let mut dfo = datafusion::prelude::JsonReadOptions::default();
    let Some(pbo) = pbo else { return Ok(dfo) };

    dfo.schema = schema;
    if let Some(schema_infer_max_records) = pbo.schema_infer_max_records {
        dfo.schema_infer_max_records = usize::try_from(schema_infer_max_records)?;
    }
    if let Some(file_extension) = pbo.file_extension.as_ref()
        && !file_extension.is_empty()
    {
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
pub(crate) fn from_proto_parquet_read_options<'a>(
    pbo: Option<&'a proto::ParquetReadOptions>,
    schema: Option<&'a Schema>,
) -> Result<datafusion::prelude::ParquetReadOptions<'a>> {
    let mut dfo = datafusion::prelude::ParquetReadOptions::default();
    let Some(pbo) = pbo else { return Ok(dfo) };

    dfo.schema = schema;
    if let Some(file_extension) = pbo.file_extension.as_ref()
        && !file_extension.is_empty()
    {
        dfo.file_extension = std::str::from_utf8(file_extension)?;
    }
    dfo.table_partition_cols = from_proto_table_partition_cols(&pbo.table_partition_cols)?;
    if let Some(parquet_pruning) = pbo.parquet_pruning {
        dfo.parquet_pruning = Some(parquet_pruning);
    }
    if let Some(skip_metadata) = pbo.skip_metadata {
        dfo.skip_metadata = Some(skip_metadata);
    }
    dfo.file_sort_order = from_proto_file_sort_order(&pbo.file_sort_order)?;

    Ok(dfo)
}

#[warn(clippy::field_reassign_with_default)]
pub(crate) fn from_proto_dataframe_write_options(
    pbo: Option<&proto::DataFrameWriteOptions>,
) -> Result<datafusion::dataframe::DataFrameWriteOptions> {
    let dfo = datafusion::dataframe::DataFrameWriteOptions::default();
    let Some(pbo) = pbo else { return Ok(dfo) };

    let mut dfo = dfo
        .with_insert_operation(from_proto_insert_op(pbo.insert_op)?)
        .with_single_file_output(pbo.single_file_output)
        .with_partition_by(pbo.partition_by.clone());

    if let Some(sort_by) = pbo.sort_by.clone() {
        dfo = dfo.with_sort_by(
            from_proto_file_sort_order(&[sort_by])?
                .first()
                .ok_or(anyhow!("Invalid sort by"))?
                .clone(),
        );
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

fn from_proto_table_partition_cols(
    table_partition_cols: &[datafusion_proto::protobuf::PartitionColumn],
) -> Result<Vec<(String, DataType)>> {
    table_partition_cols
        .iter()
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

fn from_proto_file_sort_order(
    file_sort_order: &[datafusion_proto::protobuf::SortExprNodeCollection],
) -> Result<Vec<Vec<SortExpr>>> {
    let codec = datafusion_proto::logical_plan::DefaultLogicalExtensionCodec {};
    let registry_impl = datafusion::execution::registry::MemoryFunctionRegistry::new();
    let registry: &dyn datafusion::execution::FunctionRegistry = &registry_impl;

    file_sort_order
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

                    let expr = datafusion_proto::logical_plan::from_proto::parse_expr(
                        expr_node, registry, &codec,
                    )?;

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
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion_proto::generated::datafusion::InsertOp as PbInsertOp;

    let pb = PbInsertOp::try_from(v).map_err(|_| anyhow!("invalid InsertOp value: {v}"))?;

    let df = match pb {
        PbInsertOp::Append => InsertOp::Append,
        PbInsertOp::Overwrite => InsertOp::Overwrite,
        PbInsertOp::Replace => InsertOp::Replace,
    };

    Ok(df)
}

pub(crate) fn from_proto_scalar_value_and_metadata(
    scalar_and_meta_proto: &proto::ScalarValueAndMetadata,
) -> Result<ScalarAndMetadata> {
    let scalar_proto = scalar_and_meta_proto
        .value
        .as_ref()
        .ok_or_else(|| anyhow!("Missing scalar value"))?;
    let scalar = scalar_proto.try_into()?;

    let metadata = if scalar_and_meta_proto.metadata.is_empty() {
        None
    } else {
        Some(FieldMetadata::new(
            scalar_and_meta_proto
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ))
    };

    Ok(ScalarAndMetadata {
        value: scalar,
        metadata,
    })
}

pub(crate) fn from_proto_s3_object_store(
    opts: Option<&proto::S3ObjectStoreOptions>,
    url: &url::Url,
) -> Result<object_store::aws::AmazonS3> {
    let mut builder = object_store::aws::AmazonS3Builder::from_env();

    if let Some(opts) = opts {
        builder = builder.with_bucket_name(&opts.bucket_name);
        if let Some(ref region) = opts.region {
            builder = builder.with_region(region);
        }
        if let Some(ref key) = opts.access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(ref secret) = opts.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(ref endpoint) = opts.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(ref token) = opts.token {
            builder = builder.with_token(token);
        }
        if let Some(allow_http) = opts.allow_http {
            builder = builder.with_allow_http(allow_http);
        }
        if let Some(vhost) = opts.virtual_hosted_style_request {
            builder = builder.with_virtual_hosted_style_request(vhost);
        }
        if let Some(skip_sig) = opts.skip_signature {
            builder = builder.with_skip_signature(skip_sig);
        }
    } else {
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow!("S3 URL must contain a bucket name as host"))?;
        builder = builder.with_bucket_name(bucket);
    }

    builder
        .build()
        .map_err(|e| anyhow!("Failed to build S3 object store: {e}"))
}

pub(crate) fn from_proto_azure_blob_storage(
    opts: Option<&proto::AzureBlobStorageOptions>,
    url: &url::Url,
) -> Result<object_store::azure::MicrosoftAzure> {
    let mut builder = object_store::azure::MicrosoftAzureBuilder::from_env();

    if let Some(opts) = opts {
        builder = builder.with_container_name(&opts.container_name);
        if let Some(ref v) = opts.account_name {
            builder = builder.with_account(v);
        }
        if let Some(ref v) = opts.access_key {
            builder = builder.with_access_key(v);
        }
        if let Some(ref v) = opts.bearer_token {
            builder = builder.with_bearer_token_authorization(v);
        }
        if let Some(ref v) = opts.client_id {
            builder = builder.with_client_id(v);
        }
        if let Some(ref v) = opts.client_secret {
            builder = builder.with_client_secret(v);
        }
        if let Some(ref v) = opts.tenant_id {
            builder = builder.with_tenant_id(v);
        }
        if let Some(ref v) = opts.sas_key {
            builder = builder.with_config(object_store::azure::AzureConfigKey::SasKey, v);
        }
        if let Some(ref v) = opts.endpoint {
            builder = builder.with_endpoint(v.clone());
        }
        if let Some(v) = opts.use_emulator {
            builder = builder.with_use_emulator(v);
        }
        if let Some(v) = opts.allow_http {
            builder = builder.with_allow_http(v);
        }
        if let Some(v) = opts.skip_signature {
            builder = builder.with_skip_signature(v);
        }
    } else {
        let container = url
            .host_str()
            .ok_or_else(|| anyhow!("Azure URL must contain a container name as host"))?;
        builder = builder.with_container_name(container);
    }

    builder
        .build()
        .map_err(|e| anyhow!("Failed to build Azure Blob Storage object store: {e}"))
}

pub(crate) fn from_proto_gcs_object_store(
    opts: Option<&proto::GoogleCloudStorageOptions>,
    url: &url::Url,
) -> Result<object_store::gcp::GoogleCloudStorage> {
    let mut builder = object_store::gcp::GoogleCloudStorageBuilder::from_env();

    if let Some(opts) = opts {
        builder = builder.with_bucket_name(&opts.bucket_name);
        if let Some(ref v) = opts.credentials_path {
            builder = builder.with_service_account_path(v);
        }
        if let Some(ref v) = opts.credentials {
            builder = builder.with_service_account_key(v);
        }
        if let Some(v) = opts.allow_http {
            builder = builder.with_config(
                object_store::gcp::GoogleConfigKey::Client(
                    object_store::ClientConfigKey::AllowHttp,
                ),
                v.to_string(),
            );
        }
        if let Some(v) = opts.skip_signature {
            builder = builder.with_skip_signature(v);
        }
    } else {
        let bucket = url
            .host_str()
            .ok_or_else(|| anyhow!("GCS URL must contain a bucket name as host"))?;
        builder = builder.with_bucket_name(bucket);
    }

    builder
        .build()
        .map_err(|e| anyhow!("Failed to build Google Cloud Storage object store: {e}"))
}

pub(crate) fn from_proto_http_object_store(
    opts: Option<&proto::HttpObjectStoreOptions>,
    url: &url::Url,
) -> Result<object_store::http::HttpStore> {
    let mut builder = object_store::http::HttpBuilder::new().with_url(url.as_str());

    if let Some(opts) = opts {
        if let Some(allow_http) = opts.allow_http {
            builder = builder.with_config(
                object_store::ClientConfigKey::AllowHttp,
                allow_http.to_string(),
            );
        }
        if let Some(allow_invalid_certs) = opts.allow_invalid_certificates {
            builder = builder.with_config(
                object_store::ClientConfigKey::AllowInvalidCertificates,
                allow_invalid_certs.to_string(),
            );
        }
        if !opts.headers.is_empty() {
            let mut headers = reqwest::header::HeaderMap::new();
            for (k, v) in &opts.headers {
                let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                    .map_err(|e| anyhow!("Invalid header name '{k}': {e}"))?;
                let value = reqwest::header::HeaderValue::from_str(v)
                    .map_err(|e| anyhow!("Invalid header value for '{k}': {e}"))?;
                headers.insert(name, value);
            }
            let client_options = object_store::ClientOptions::new().with_default_headers(headers);
            builder = builder.with_client_options(client_options);
        }
    }

    builder
        .build()
        .map_err(|e| anyhow!("Failed to build HTTP object store: {e}"))
}

pub(crate) fn from_proto_param_values(values: &proto::DataFrameParamValues) -> Result<ParamValues> {
    let values = values
        .values
        .as_ref()
        .ok_or_else(|| anyhow!("Missing parameter values"))?;

    match values {
        Values::Positional(p) => p
            .values
            .iter()
            .map(from_proto_scalar_value_and_metadata)
            .collect::<Result<Vec<_>>>()
            .map(ParamValues::List),
        Values::Named(n) => n
            .values
            .iter()
            .map(|(k, v)| Ok((k.clone(), from_proto_scalar_value_and_metadata(v)?)))
            .collect::<Result<HashMap<_, _>>>()
            .map(ParamValues::Map),
    }
}
