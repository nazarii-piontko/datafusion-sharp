use std::sync::Arc;
use log::{debug, error, trace};
use prost::Message;

use crate::{proto, BytesData};

use crate::{
    mappers,
    ErrorCode,
    ErrorInfo
};

pub struct SessionContextWrapper {
    runtime: crate::RuntimeHandle,
    inner: Arc<datafusion::prelude::SessionContext>
}

impl SessionContextWrapper {
    fn new(runtime: crate::RuntimeHandle) -> Self {
        Self {
            runtime,
            inner: Arc::new(datafusion::prelude::SessionContext::new())
        }
    }
}

/// Creates a new `SessionContext` bound to a runtime.
///
/// # Safety
/// - `runtime_ptr` must be a valid pointer returned by `datafusion_runtime_new`
/// - `context_ptr` must be a valid, aligned, non-null pointer to writable memory
/// - Caller must call `datafusion_context_destroy` exactly once with the returned pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_new(runtime_ptr: *mut crate::RuntimeHandle, context_ptr: *mut *mut SessionContextWrapper) -> ErrorCode {
    if context_ptr.is_null() {
        return ErrorCode::InvalidArgument;
    }

    let runtime_handle = ffi_ref!(runtime_ptr);

    let context = Box::new(SessionContextWrapper::new(Arc::clone(runtime_handle)));
    let raw_ptr = Box::into_raw(context);
    unsafe { *context_ptr = raw_ptr; }

    debug!("Created session context {raw_ptr:p}");

    ErrorCode::Ok
}

/// Destroys a `SessionContext` created by `datafusion_context_new`.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`, or null
/// - Caller must not use `context_ptr` after this call
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_destroy(context_ptr: *mut SessionContextWrapper) -> ErrorCode {
    debug!("Destroying session context {context_ptr:p}");

    if !context_ptr.is_null() {
        unsafe { drop(Box::from_raw(context_ptr)) };
    }

    ErrorCode::Ok
}

/// Registers a CSV file as a table in the `SessionContext`.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `table_ref_ptr` must be a valid null-terminated UTF-8 string
/// - `table_path_ptr` must be a valid null-terminated UTF-8 string
/// - `csv_options_bytes_ptr` must be a valid pointer to `BytesData` containing a Flatbuffers-encoded `CsvReadOptions` struct
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_csv(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    table_path_ptr: *const std::ffi::c_char,
    csv_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);
    let table_path = ffi_cstr_to_string!(table_path_ptr);

    debug!("Registering CSV table '{table_ref}' from '{table_path}' on session {context_ptr:p}");

    let csv_options_proto = match csv_options_bytes.as_opt_slice() {
        Some(b) => match proto::CsvReadOptions::decode(b) {
            Ok(opts) => Some(opts),
            Err(e) => {
                error!("Failed to decode CSV options protobuf: {e}");
                return ErrorCode::InvalidArgument;
            }
        },
        None => None
    };

    context.runtime.spawn(async move {
        let schema_opt = match mappers::from_proto_schema(
            csv_options_proto.as_ref().and_then(|o| o.schema.as_ref())
        ) {
            Ok(s) => s,
            Err(e) => {
                let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to parse CSV schema from options: {e}"));
                crate::invoke_callback_error(&error_info, callback, user_data);
                return;
            }
        };
        
        match mappers::from_proto_csv_options(csv_options_proto.as_ref(), schema_opt.as_ref()) {
            Ok(opts) => {
                let result = context.inner
                    .register_csv(&table_ref, &table_path, opts)
                    .await
                    .map_err(|e| ErrorInfo::new(ErrorCode::TableRegistrationFailed, e));

                crate::invoke_callback(result, callback, user_data);
            },
            Err(e) => {
                let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to convert CSV options: {e}"));
                crate::invoke_callback_error(&error_info, callback, user_data);
            }
        }
    });

    ErrorCode::Ok
}

/// Registers a JSON file as a table in the `SessionContext`.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `table_ref_ptr` must be a valid null-terminated UTF-8 string
/// - `table_path_ptr` must be a valid null-terminated UTF-8 string
/// - `json_options_bytes` must be a valid `BytesData` containing a protobuf-encoded `JsonReadOptions`, or null
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_json(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    table_path_ptr: *const std::ffi::c_char,
    json_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);
    let table_path = ffi_cstr_to_string!(table_path_ptr);

    debug!("Registering JSON table '{table_ref}' from '{table_path}' on session {context_ptr:p}");

    let json_options_proto = match json_options_bytes.as_opt_slice() {
        Some(b) => match proto::JsonReadOptions::decode(b) {
            Ok(opts) => Some(opts),
            Err(e) => {
                error!("Failed to decode JSON options protobuf: {e}");
                return ErrorCode::InvalidArgument;
            }
        },
        None => None
    };

    context.runtime.spawn(async move {
        let schema_opt = match mappers::from_proto_schema(
            json_options_proto.as_ref().and_then(|o| o.schema.as_ref())
        ) {
            Ok(s) => s,
            Err(e) => {
                let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to parse JSON schema from options: {e}"));
                crate::invoke_callback_error(&error_info, callback, user_data);
                return;
            }
        };

        match mappers::from_proto_json_read_options(json_options_proto.as_ref(), schema_opt.as_ref()) {
            Ok(opts) => {
                let result = context.inner
                    .register_json(&table_ref, &table_path, opts)
                    .await
                    .map_err(|e| ErrorInfo::new(ErrorCode::TableRegistrationFailed, e));

                crate::invoke_callback(result, callback, user_data);
            },
            Err(e) => {
                let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to convert JSON options: {e}"));
                crate::invoke_callback_error(&error_info, callback, user_data);
            }
        }
    });

    ErrorCode::Ok
}

/// Registers a Parquet file as a table in the `SessionContext`.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `table_ref_ptr` must be a valid null-terminated UTF-8 string
/// - `table_path_ptr` must be a valid null-terminated UTF-8 string
/// - `parquet_options_bytes` must be a valid `BytesData` containing a protobuf-encoded `ParquetReadOptions`, or null
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_parquet(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    table_path_ptr: *const std::ffi::c_char,
    parquet_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);
    let table_path = ffi_cstr_to_string!(table_path_ptr);

    debug!("Registering Parquet table '{table_ref}' from '{table_path}' on session {context_ptr:p}");

    let parquet_options_proto = match parquet_options_bytes.as_opt_slice() {
        Some(b) => match proto::ParquetReadOptions::decode(b) {
            Ok(opts) => Some(opts),
            Err(e) => {
                error!("Failed to decode Parquet options protobuf: {e}");
                return ErrorCode::InvalidArgument;
            }
        },
        None => None
    };

    context.runtime.spawn(async move {
        let schema_opt = match mappers::from_proto_schema(
            parquet_options_proto.as_ref().and_then(|o| o.schema.as_ref())
        ) {
            Ok(s) => s,
            Err(e) => {
                let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to parse Parquet schema from options: {e}"));
                crate::invoke_callback_error(&error_info, callback, user_data);
                return;
            }
        };

        match mappers::from_proto_parquet_read_options(parquet_options_proto.as_ref(), schema_opt.as_ref()) {
            Ok(opts) => {
                let result = context.inner
                    .register_parquet(&table_ref, &table_path, opts)
                    .await
                    .map_err(|e| ErrorInfo::new(ErrorCode::TableRegistrationFailed, e));

                crate::invoke_callback(result, callback, user_data);
            },
            Err(e) => {
                let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to convert Parquet options: {e}"));
                crate::invoke_callback_error(&error_info, callback, user_data);
            }
        }
    });

    ErrorCode::Ok
}

/// Registers an Arrow `RecordBatch` as a table in the `SessionContext`.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `table_ref_ptr` must be a valid null-terminated UTF-8 string
/// - `batch_ipc_bytes` must be a valid `BytesData` containing an Arrow IPC stream with a single `RecordBatch`
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_batch(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    batch_ipc_bytes: BytesData,
    callback: crate::Callback,
    user_data: u64
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);

    debug!("Registering table '{table_ref}' from DataFrame on session {context_ptr:p}");

    let result = datafusion::arrow::ipc::reader::StreamReader::try_new(batch_ipc_bytes.as_slice(), None)
        .map(|mut reader| {
            reader.next()
                .map(|batch| {
                    match batch {
                        Ok(batch) => {
                            context.inner
                                .register_batch(&table_ref, batch)
                                .map_err(|e| ErrorInfo::new(ErrorCode::TableRegistrationFailed, e))
                                .map(|_| ())
                        }
                        Err(e) => Err(ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to read RecordBatch from Arrow IPC stream: {e}")))
                    }
                })
                .unwrap_or(Err(ErrorInfo::new(ErrorCode::InvalidArgument, format!("Arrow IPC stream did not contain any RecordBatch for table '{table_ref}'"))))
        })
        .map_err(|e| ErrorInfo::new(ErrorCode::InvalidArgument, format!("Failed to create Arrow IPC stream reader: {e}")))
        .flatten();

    crate::invoke_callback(result, callback, user_data);

    ErrorCode::Ok
}

/// Deregisters a table from the `SessionContext` by name.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `table_ref_ptr` must be a valid null-terminated UTF-8 string with the name of a registered table
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_deregister_table(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    callback: crate::Callback,
    user_data: u64
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);

    debug!("Deregistering table '{table_ref}' from session {context_ptr:p}");

    let result = context.inner
        .deregister_table(&table_ref)
        .map_err(|e| ErrorInfo::new(ErrorCode::TableRegistrationFailed, e))
        .map(|_| ());

    crate::invoke_callback(result, callback, user_data);

    debug!("Deregistered table '{table_ref}' from session {context_ptr:p}");

    ErrorCode::Ok
}

/// Executes a SQL query and returns a `DataFrame`.
///
/// This is an async operation. The callback is invoked on completion with a `DataFrame` pointer.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `sql_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
/// - Caller must call `datafusion_dataframe_destroy` on the returned `DataFrame` pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_sql(
    context_ptr: *mut SessionContextWrapper,
    sql_ptr: *const std::ffi::c_char,
    param_values_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);
    let sql = ffi_cstr_to_string!(sql_ptr);

    let Ok(param_values_proto) = param_values_bytes.as_opt_slice()
        .map(proto::DataFrameParamValues::decode).transpose() else { return ErrorCode::InvalidArgument };
    let Ok(param_values) = param_values_proto.as_ref()
        .map(mappers::from_proto_param_values).transpose() else { return ErrorCode::InvalidArgument };

    trace!("Executing SQL query: {sql} on session {context_ptr:p}");

    let context_ptr_addr = context_ptr as usize;
    context.runtime.spawn(async move {
        let result = context.inner
            .sql(&sql)
            .await
            .and_then(|df| {
                let df = match param_values {
                    Some(p) => df.with_param_values(p),
                    _ => Ok(df)
                }?;

                Ok(crate::dataframe_to_ptr(&context.runtime, df))
            })
            .map_err(|e| ErrorInfo::new(ErrorCode::SqlError, e));

        trace!("Executed SQL query: {sql} on session 0x{context_ptr_addr:x}");

        crate::invoke_callback(result, callback, user_data);
    });

    ErrorCode::Ok
}

/// Registers a local filesystem object store for the given URL.
///
/// This is a synchronous operation.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `url_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_object_store_local(
    context_ptr: *mut SessionContextWrapper,
    url_ptr: *const std::ffi::c_char,
    callback: crate::Callback,
    user_data: u64
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);

    let url = ffi_cstr_to_string!(url_ptr);
    let Ok(url) = url::Url::parse(&url) else { return ErrorCode::InvalidArgument };

    debug!("Registering local object store for '{url}' on session {context_ptr:p}");

    let store = match object_store::local::LocalFileSystem::new_with_prefix(url.path()) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to register local object store: {e}");
            let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, e);
            crate::invoke_callback_error(&error_info, callback, user_data);
            return ErrorCode::Ok;
        }
    };

    context.inner.register_object_store(&url, Arc::new(store));

    crate::invoke_callback_null_result(callback, user_data);

    ErrorCode::Ok
}

/// Registers an S3 object store for the given URL.
///
/// This is a synchronous operation. The callback is invoked with the result.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `url_ptr` must be a valid null-terminated UTF-8 string
/// - `s3_options_bytes` must be a valid `BytesData` containing a protobuf-encoded `S3ObjectStoreOptions`, or null
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_object_store_s3(
    context_ptr: *mut SessionContextWrapper,
    url_ptr: *const std::ffi::c_char,
    s3_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64,
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);

    let url = ffi_cstr_to_string!(url_ptr);
    let Ok(url) = url::Url::parse(&url) else { return ErrorCode::InvalidArgument };

    let s3_options_proto = match s3_options_bytes.as_opt_slice() {
        Some(b) => match proto::S3ObjectStoreOptions::decode(b) {
            Ok(opts) => Some(opts),
            Err(_) => return ErrorCode::InvalidArgument,
        },
        None => None,
    };
    debug!("Registering S3 object store for '{url}' on session {context_ptr:p}");

    let store = match mappers::from_proto_s3_object_store(s3_options_proto.as_ref(), &url) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to register S3 object store: {e}");
            let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, e);
            crate::invoke_callback_error(&error_info, callback, user_data);
            return ErrorCode::Ok;
        }
    };

    context.inner.register_object_store(&url, Arc::new(store));

    crate::invoke_callback_null_result(callback, user_data);

    ErrorCode::Ok
}

/// Registers an Azure Blob Storage object store for the given URL.
///
/// This is a synchronous operation. The callback is invoked with the result.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `url_ptr` must be a valid null-terminated UTF-8 string
/// - `azure_options_bytes` must be a valid `BytesData` containing a protobuf-encoded `AzureBlobStorageOptions`, or null
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_object_store_azure(
    context_ptr: *mut SessionContextWrapper,
    url_ptr: *const std::ffi::c_char,
    azure_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64,
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);

    let url = ffi_cstr_to_string!(url_ptr);
    let Ok(url) = url::Url::parse(&url) else { return ErrorCode::InvalidArgument };

    let azure_options_proto = match azure_options_bytes.as_opt_slice() {
        Some(b) => match proto::AzureBlobStorageOptions::decode(b) {
            Ok(opts) => Some(opts),
            Err(_) => return ErrorCode::InvalidArgument,
        },
        None => None,
    };
    debug!("Registering Azure object store for '{url}' on session {context_ptr:p}");

    let store = match mappers::from_proto_azure_blob_storage(azure_options_proto.as_ref(), &url) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to register Azure object store: {e}");
            let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, e);
            crate::invoke_callback_error(&error_info, callback, user_data);
            return ErrorCode::Ok;
        }
    };

    context.inner.register_object_store(&url, Arc::new(store));

    crate::invoke_callback_null_result(callback, user_data);

    ErrorCode::Ok
}

/// Registers a Google Cloud Storage object store for the given URL.
///
/// This is a synchronous operation. The callback is invoked with the result.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `url_ptr` must be a valid null-terminated UTF-8 string
/// - `gcs_options_bytes` must be a valid `BytesData` containing a protobuf-encoded `GoogleCloudStorageOptions`, or null
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_object_store_gcs(
    context_ptr: *mut SessionContextWrapper,
    url_ptr: *const std::ffi::c_char,
    gcs_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64,
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);

    let url = ffi_cstr_to_string!(url_ptr);
    let Ok(url) = url::Url::parse(&url) else { return ErrorCode::InvalidArgument };

    let gcs_options_proto = match gcs_options_bytes.as_opt_slice() {
        Some(b) => match proto::GoogleCloudStorageOptions::decode(b) {
            Ok(opts) => Some(opts),
            Err(_) => return ErrorCode::InvalidArgument,
        },
        None => None,
    };
    debug!("Registering GCS object store for '{url}' on session {context_ptr:p}");

    let store = match mappers::from_proto_gcs_object_store(gcs_options_proto.as_ref(), &url) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to register GCS object store: {e}");
            let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, e);
            crate::invoke_callback_error(&error_info, callback, user_data);
            return ErrorCode::Ok;
        }
    };

    context.inner.register_object_store(&url, Arc::new(store));

    crate::invoke_callback_null_result(callback, user_data);

    ErrorCode::Ok
}

/// Registers an HTTP object store for the given URL.
///
/// This is a synchronous operation. The callback is invoked with the result.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `url_ptr` must be a valid null-terminated UTF-8 string
/// - `http_options_bytes` must be a valid `BytesData` containing a protobuf-encoded `HttpObjectStoreOptions`, or null
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_object_store_http(
    context_ptr: *mut SessionContextWrapper,
    url_ptr: *const std::ffi::c_char,
    http_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64,
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);

    let url = ffi_cstr_to_string!(url_ptr);
    let Ok(url) = url::Url::parse(&url) else { return ErrorCode::InvalidArgument };

    let http_options_proto = match http_options_bytes.as_opt_slice() {
        Some(b) => match proto::HttpObjectStoreOptions::decode(b) {
            Ok(opts) => Some(opts),
            Err(_) => return ErrorCode::InvalidArgument,
        },
        None => None,
    };
    debug!("Registering HTTP object store for '{url}' on session {context_ptr:p}");

    let store = match mappers::from_proto_http_object_store(http_options_proto.as_ref(), &url) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to register HTTP object store: {e}");
            let error_info = ErrorInfo::new(ErrorCode::InvalidArgument, e);
            crate::invoke_callback_error(&error_info, callback, user_data);
            return ErrorCode::Ok;
        }
    };

    context.inner.register_object_store(&url, Arc::new(store));

    crate::invoke_callback_null_result(callback, user_data);

    ErrorCode::Ok
}

/// Deregisters an object store for the given URL.
///
/// This is a synchronous operation. The callback is invoked with the result.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `url_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_deregister_object_store(
    context_ptr: *mut SessionContextWrapper,
    url_ptr: *const std::ffi::c_char,
    callback: crate::Callback,
    user_data: u64,
) -> ErrorCode {
    let context = ffi_ref!(context_ptr);

    let url = ffi_cstr_to_string!(url_ptr);
    let Ok(url) = url::Url::parse(&url) else { return ErrorCode::InvalidArgument };

    debug!("Deregistering object store for '{url}' from session {context_ptr:p}");

    let result = context.inner
        .deregister_object_store(&url)
        .map(|_| ())
        .map_err(|e| ErrorInfo::new(ErrorCode::InvalidArgument, e));

    crate::invoke_callback(result, callback, user_data);

    ErrorCode::Ok
}
