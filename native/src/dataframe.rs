use std::sync::Arc;
use futures::StreamExt;
use prost::Message;

pub struct DataFrameWrapper {
    runtime: crate::RuntimeHandle,
    inner: datafusion::prelude::DataFrame,
}

impl DataFrameWrapper {
    pub fn new(runtime: crate::RuntimeHandle, inner: datafusion::prelude::DataFrame) -> Self {
        Self {
            runtime,
            inner,
        }
    }
}

/// Destroys a `DataFrame` and frees its resources.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions, or null
/// - Caller must not use `df_ptr` after this call
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_destroy(df_ptr: *mut DataFrameWrapper) -> crate::ErrorCode {
    if !df_ptr.is_null() {
        unsafe { drop(Box::from_raw(df_ptr)) };
    }

    crate::ErrorCode::Ok
}

/// Counts the number of rows in the `DataFrame`.
///
/// This is an async operation. The callback is invoked on completion with the row count as u64.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_count(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    dev_msg!("Executing count on DataFrame: {:p}", df_ptr);

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();
        let result = df
            .count()
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e))
            .map(|s| s as u64);

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Prints the `DataFrame` contents to stdout.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
///
/// # Parameters
/// - `limit`: Maximum number of rows to display (0 = no limit)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_show(
    df_ptr: *mut DataFrameWrapper,
    limit: u64,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    dev_msg!("Executing show on DataFrame: {:p}", df_ptr);

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();
        let result = if limit > 0 {
            #[allow(clippy::cast_possible_truncation)]
            df.show_limit(limit as usize).await
        } else {
            df.show().await
        }.map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Converts the `DataFrame` to a string representation.
///
/// This is an async operation. The callback is invoked on completion with the string as bytes.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_to_string(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    dev_msg!("Executing to_string on DataFrame: {:p}", df_ptr);

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();
        let result = df
            .to_string()
            .await;

        match result {
            Ok(s) => {
                let data = crate::BytesData::new(s.as_bytes());
                crate::invoke_callback(Ok(data), callback, user_data);
            }
            Err(err) => {
                let err_info = crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, err);
                crate::invoke_callback(Err::<crate::BytesData, _>(err_info), callback, user_data);
            }
        }
    });

    crate::ErrorCode::Ok
}

/// Returns the `DataFrame` schema as a serialized Arrow IPC stream.
///
/// This is a synchronous operation. The callback is invoked immediately with the schema bytes.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from the current thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_schema(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    let df = &df_wrapper.inner;
    let schema = df.schema().as_arrow();
    let ffi_schema = arrow_array::ffi::FFI_ArrowSchema::try_from(schema)
        .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

    crate::invoke_callback(ffi_schema, callback, user_data);

    crate::ErrorCode::Ok
}

/// Struct to hold collected record batches in FFI-compatible format.
#[repr(C)]
pub struct CollectedData {
    pub schema: *const arrow_array::ffi::FFI_ArrowSchema,
    pub num_batches: i32,
    pub batches: *const arrow_array::ffi::FFI_ArrowArray, // Contiguous array of FFI_ArrowArray, one per batch
}

/// Materializes all records as a serialized Arrow IPC stream.
///
/// This is an async operation. The callback is invoked on completion with the serialized bytes.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_collect(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();

        let ffi_schema = match convert_schema_to_ffi(&df) {
            Ok(s) => s,
            Err(e) => {
                crate::invoke_callback_error(&e, callback, user_data);
                return;
            }
        };

        let Ok(batches) = df.collect().await else {
            let error = crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, "Failed to collect record batches");
            crate::invoke_callback_error(&error, callback, user_data);
            return;
        };
        let ffi_batches = batches.iter().map(convert_batch_to_ffi).collect::<Vec<_>>();

        let Ok(num_batches) = i32::try_from(ffi_batches.len()) else {
            let error = crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, "Too many record batches to fit in i32");
            crate::invoke_callback_error(&error, callback, user_data);
            return;
        };

        let result = CollectedData {
            schema: &raw const ffi_schema,
            num_batches,
            batches: ffi_batches.as_ptr(),
        };

        crate::invoke_callback_success(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

pub struct DataFrameStreamWrapper {
    runtime: crate::RuntimeHandle,
    stream: datafusion::execution::SendableRecordBatchStream
}

#[repr(C)]
pub struct ExecutedStreamData {
    pub stream_ptr: *mut DataFrameStreamWrapper,
    pub schema: *const arrow_array::ffi::FFI_ArrowSchema,
}

/// Executes the `DataFrame` and returns a stream of record batches as serialized Arrow IPC data.
///
/// This is an async operation. The callback is invoked on completion with a pointer to a `DataFrameStreamWrapper`.
/// The caller can then call `datafusion_dataframe_stream_next` to retrieve each batch as bytes.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `callback` must be valid to call from any thread
/// - Caller must call `datafusion_dataframe_stream_destroy` on the returned stream pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_execute_stream(
    df_ptr: *mut DataFrameWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);

    dev_msg!("Executing dataframe stream on DataFrame: {:p}", df_ptr);

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();

        let ffi_schema = match convert_schema_to_ffi(&df) {
            Ok(s) => s,
            Err(e) => {
                crate::invoke_callback_error(&e, callback, user_data);
                return;
            }
        };

        let Ok(stream) = df.execute_stream().await else {
            let error = crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, "Failed to execute dataframe stream");
            crate::invoke_callback_error(&error, callback, user_data);
            return;
        };

        let stream_w = Box::into_raw(Box::new(DataFrameStreamWrapper {
            runtime: Arc::clone(&df_wrapper.runtime),
            stream,
        }));

        let result = ExecutedStreamData {
            stream_ptr: stream_w,
            schema: &raw const ffi_schema,
        };

        dev_msg!("Successfully executed dataframe stream on DataFrame: {:p}, stream wrapper pointer: {:p}", df_wrapper, stream_w);

        crate::invoke_callback_success(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Destroys a `DataFrameStreamWrapper` and frees its resources.
///
/// # Safety
/// - `stream_ptr` must be a valid pointer returned by `datafusion_dataframe_execute_stream`, or null
/// - Caller must not use `stream_ptr` after this call
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_stream_destroy(
    stream_ptr: *mut DataFrameStreamWrapper
) -> crate::ErrorCode {
    dev_msg!("Destroying dataframe_stream: {:p}", stream_ptr);

    if !stream_ptr.is_null() {
        unsafe { drop(Box::from_raw(stream_ptr)) };
    }

    crate::ErrorCode::Ok
}

/// Retrieves the next record batch from the stream as serialized Arrow IPC data.
///
/// This is an async operation. The callback is invoked on completion with the batch bytes, or null if the stream has ended.
///
/// The caller should call this function repeatedly until it returns null to retrieve all batches.
///
/// # Safety
/// - `stream_ptr` must be a valid pointer returned by `datafusion_dataframe_execute_stream`
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_stream_next(
    stream_ptr: *mut DataFrameStreamWrapper,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let stream_wrapper = ffi_ref_mut!(stream_ptr);
    
    let runtime = Arc::clone(&stream_wrapper.runtime);

    runtime.spawn(async move {
        match stream_wrapper.stream.next().await {
            Some(result) => match result {
                Ok(batch) => {
                    let ffi_batch = convert_batch_to_ffi(&batch);
                    crate::invoke_callback_success(ffi_batch, callback, user_data);
                },
                Err(err) => {
                    let error = crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, err);
                    crate::invoke_callback_error(&error, callback, user_data);
                }
            },
            None => crate::invoke_callback_null_result(callback, user_data)
        }
    });

    crate::ErrorCode::Ok
}

/// Writes the `DataFrame` to a CSV file.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `path_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_write_csv(
    df_ptr: *mut DataFrameWrapper,
    path_ptr: *const std::ffi::c_char,
    csv_options_bytes: crate::BytesData,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);
    let path = ffi_cstr_to_string!(path_ptr);

    let Ok(csv_options) = csv_options_bytes.as_opt_slice()
        .map(|b| datafusion_proto::protobuf::CsvOptions::decode(b)
            .map(|pbo| datafusion::common::config::CsvOptions::from(&pbo))
            .map_err(|_| crate::ErrorCode::InvalidArgument)
        )
        .transpose() else { return crate::ErrorCode::InvalidArgument };

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();
        let result = df
            .write_csv(&path, datafusion::dataframe::DataFrameWriteOptions::default(), csv_options)
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Writes the `DataFrame` to a JSON file.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `path_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_write_json(
    df_ptr: *mut DataFrameWrapper,
    path_ptr: *const std::ffi::c_char,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);
    let path = ffi_cstr_to_string!(path_ptr);

    dev_msg!("Executing write_json on DataFrame: {:p} to path: {}", df_ptr, path);

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();
        let result = df
            .write_json(&path, datafusion::dataframe::DataFrameWriteOptions::default(), None)
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

        dev_msg!("Finished executing write_json");

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Writes the `DataFrame` to a Parquet file.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `df_ptr` must be a valid pointer returned by other public functions
/// - `path_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_dataframe_write_parquet(
    df_ptr: *mut DataFrameWrapper,
    path_ptr: *const std::ffi::c_char,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let df_wrapper = ffi_ref!(df_ptr);
    let path = ffi_cstr_to_string!(path_ptr);

    dev_msg!("Executing write_parquet on DataFrame: {:p} to path: {}", df_ptr, path);

    df_wrapper.runtime.spawn(async move {
        let df = df_wrapper.inner.clone();
        let result = df
            .write_parquet(&path, datafusion::dataframe::DataFrameWriteOptions::default(), None)
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

        dev_msg!("Finished executing write_parquet");

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Helper function to convert a `DataFrame` schema to FFI format.
fn convert_schema_to_ffi(df: &datafusion::dataframe::DataFrame) -> Result<arrow_array::ffi::FFI_ArrowSchema, crate::ErrorInfo> {
    let schema = df.schema();
    arrow_array::ffi::FFI_ArrowSchema::try_from(schema.as_arrow())
        .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, format!("Failed to convert schema to FFI format: {e}")))
}

/// Helper function to convert a `RecordBatch` to FFI format.
fn convert_batch_to_ffi(batch: &arrow_array::RecordBatch) -> arrow_array::ffi::FFI_ArrowArray {
    use arrow_array::Array;

    let fields = batch.schema().fields().clone();
    let arrays = batch.columns().to_vec();
    let st = arrow_array::StructArray::new(fields, arrays, None);

    arrow_array::ffi::FFI_ArrowArray::new(&st.to_data())
}