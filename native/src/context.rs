use std::sync::Arc;
use crate::wire;

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
pub unsafe extern "C" fn datafusion_context_new(runtime_ptr: *mut crate::RuntimeHandle, context_ptr: *mut *mut SessionContextWrapper) -> crate::ErrorCode {
    if context_ptr.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let runtime_handle = ffi_ref!(runtime_ptr);

    let context = Box::new(SessionContextWrapper::new(Arc::clone(runtime_handle)));
    unsafe { *context_ptr = Box::into_raw(context); }

    dev_msg!("Successfully created context: {:p}", unsafe { *context_ptr });

    crate::ErrorCode::Ok
}

/// Destroys a `SessionContext` created by `datafusion_context_new`.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`, or null
/// - Caller must not use `context_ptr` after this call
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_destroy(context_ptr: *mut SessionContextWrapper) -> crate::ErrorCode {
    dev_msg!("Destroying context: {:p}", context_ptr);

    if !context_ptr.is_null() {
        unsafe { drop(Box::from_raw(context_ptr)) };
    }

    crate::ErrorCode::Ok
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
    csv_options_bytes_ptr: *const crate::BytesData,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);
    let table_path = ffi_cstr_to_string!(table_path_ptr);
    let csv_options_bytes: Vec<u8> = ffi_ref!(csv_options_bytes_ptr).as_slice().to_vec(); // Make a copy of the bytes to move into the async task.

    let inner = Arc::clone(&context.inner);

    dev_msg!("Registering CSV table '{}' from path '{}'", table_ref, table_path);

    context.runtime.spawn(async move {
        let o = match flatbuffers::root::<wire::CsvReadOptions>(csv_options_bytes.as_slice()) {
            Ok(o) => o,
            Err(e) => {
                crate::invoke_callback_error(&crate::ErrorInfo::new(crate::ErrorCode::InvalidArgument, e), callback, user_data);
                return;
            }
        };

        let schema: Option<datafusion::arrow::datatypes::Schema> = if let Some(schema_serialized) = o.schema_serialized() {
            match datafusion::arrow::ipc::reader::StreamReader::try_new(schema_serialized.bytes(), None)
            {
                Ok(reader) => Some(reader.schema().as_ref().clone()),
                Err(e) => {
                    crate::invoke_callback_error(&crate::ErrorInfo::new(crate::ErrorCode::InvalidArgument, e), callback, user_data);
                    return;
                }
            }
        } else {
            None
        };

        let mut opts = datafusion::prelude::CsvReadOptions::new();
        opts.has_header = o.has_header();
        if let Some(delimiter) = o.delimiter() { opts.delimiter = delimiter; }
        if let Some(quote) = o.quote() { opts.quote = quote; }
        opts.terminator = o.terminator();
        opts.escape = o.escape();
        opts.comment = o.comment();
        opts.newlines_in_values = o.newlines_in_values();
        opts.schema = schema.as_ref();

        let result = inner
            .register_csv(&table_ref, &table_path, opts)
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::TableRegistrationFailed, e));

        dev_msg!("Finished registering CSV table '{}' from path '{}'", table_ref, table_path);

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Registers a JSON file as a table in the `SessionContext`.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `table_ref_ptr` must be a valid null-terminated UTF-8 string
/// - `table_path_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_json(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    table_path_ptr: *const std::ffi::c_char,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);
    let table_path = ffi_cstr_to_string!(table_path_ptr);

    let inner = Arc::clone(&context.inner);

    dev_msg!("Registering JSON table '{}' from path '{}'", table_ref, table_path);

    context.runtime.spawn(async move {
        let result = inner
            .register_json(&table_ref, &table_path, datafusion::prelude::NdJsonReadOptions::default())
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::TableRegistrationFailed, e));

        dev_msg!("Finished registering JSON table '{}' from path '{}'", table_ref, table_path);

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}

/// Registers a Parquet file as a table in the `SessionContext`.
///
/// This is an async operation. The callback is invoked on completion with no result data.
///
/// # Safety
/// - `context_ptr` must be a valid pointer returned by `datafusion_context_new`
/// - `table_ref_ptr` must be a valid null-terminated UTF-8 string
/// - `table_path_ptr` must be a valid null-terminated UTF-8 string
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_context_register_parquet(
    context_ptr: *mut SessionContextWrapper,
    table_ref_ptr: *const std::ffi::c_char,
    table_path_ptr: *const std::ffi::c_char,
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let context = ffi_ref!(context_ptr);
    let table_ref = ffi_cstr_to_string!(table_ref_ptr);
    let table_path = ffi_cstr_to_string!(table_path_ptr);

    let inner = Arc::clone(&context.inner);

    dev_msg!("Registering Parquet table '{}' from path '{}'", table_ref, table_path);

    context.runtime.spawn(async move {
        let result = inner
            .register_parquet(&table_ref, &table_path, datafusion::prelude::ParquetReadOptions::default())
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::TableRegistrationFailed, e));

        dev_msg!("Finished registering Parquet table '{}' from path '{}'", table_ref, table_path);

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
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
    callback: crate::Callback,
    user_data: u64
) -> crate::ErrorCode {
    let context = ffi_ref!(context_ptr);
    let sql = ffi_cstr_to_string!(sql_ptr);

    dev_msg!("Executing SQL query: {}", sql);

    context.runtime.spawn(async move {
        let result = context.inner
            .sql(&sql)
            .await
            .map(|dataframe| {
                let data_frame = Box::new(crate::DataFrameWrapper::new(Arc::clone(&context.runtime), dataframe));
                Box::into_raw(data_frame)
            })
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::SqlError, e));

        dev_msg!("Finished executing SQL query: {}, dataframe ptr: {:p}", sql, result.as_ref().ok().map_or(std::ptr::null(), |ptr| *ptr));

        crate::invoke_callback(result, callback, user_data);
    });

    crate::ErrorCode::Ok
}
