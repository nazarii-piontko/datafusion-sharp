use std::sync::Arc;

use log::{debug, error, warn};

use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

use crate::{BytesData, Callback, ErrorCode};
use crate::error::ErrorInfo;

pub struct InMemoryStoreWrapper {
    runtime: crate::RuntimeHandle,
    inner: Arc<dyn ObjectStore>
}

impl InMemoryStoreWrapper {
    fn new(runtime: &crate::RuntimeHandle) -> Self {
        Self {
            runtime: Arc::clone(runtime),
            inner: Arc::new(object_store::memory::InMemory::new())
        }
    }
}

/// Creates a new in memory store.
///
/// # Safety
/// - `runtime_ptr` must be a valid pointer returned by `datafusion_runtime_new`
/// - `store_ptr` must be a valid pointer to a pointer that will receive the store pointer
/// - Caller must call `datafusion_in_memory_store_destroy` exactly once with the returned pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_in_memory_store_new(
    runtime_ptr: *mut crate::RuntimeHandle,
    store_ptr: *mut *mut InMemoryStoreWrapper
) -> ErrorCode {
    if store_ptr.is_null() {
        error!("Received null output pointer for store");
        return ErrorCode::InvalidArgument;
    }

    let runtime_handle = ffi_ref!(runtime_ptr);

    let store = Box::new(InMemoryStoreWrapper::new(runtime_handle));
    let store_raw_ptr = Box::into_raw(store);
    unsafe { *store_ptr = store_raw_ptr; }

    debug!("Created in-memory store {store_raw_ptr:p}");

    ErrorCode::Ok
}

/// Destroys a `InMemoryStoreWrapper` created by `datafusion_in_memory_store_new`.
///
/// # Safety
/// - `store_ptr` must be a valid pointer returned by `datafusion_in_memory_store_new`, or null
/// - Caller must not use `store_ptr` after this call
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_in_memory_store_destroy(store_ptr: *mut InMemoryStoreWrapper) -> ErrorCode {
    debug!("Destroying in-memory store {store_ptr:p}");

    if store_ptr.is_null() {
        warn!("Received null output pointer for store");
    } else {
        unsafe { drop(Box::from_raw(store_ptr)) };
    }

    ErrorCode::Ok
}

/// Puts data into the in-memory store at the specified path.
///
/// # Safety
/// - `store_ptr` must be a valid pointer returned by `datafusion_in_memory_store_new`
/// - `path_ptr` must be a valid null-terminated C string representing the object path
/// - `data_bytes` must point to valid memory for the duration of this call if copy is `true` or for the duration of the store if copy is `false`
/// - If `copy` is false, the data behind `data_bytes` must remain valid for the lifetime of the store
/// - `callback` will be invoked exactly once when the operation completes
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_in_memory_store_put(
    store_ptr: *mut InMemoryStoreWrapper,
    path_ptr: *const std::ffi::c_char,
    data_bytes: BytesData,
    copy: bool,
    callback: Callback,
    user_data: u64
) -> ErrorCode {
    let store_wrapper = ffi_ref!(store_ptr);
    let path_str = ffi_cstr_to_string!(path_ptr);

    debug!("Putting data to in-memory store {store_ptr:p} at path '{path_str}' with data length {}, copy={copy}", data_bytes.len());

    let store = Arc::clone(&store_wrapper.inner);

    let bytes = if copy {
        bytes::Bytes::from(data_bytes.as_slice().to_owned())
    } else {
        bytes::Bytes::from_static(data_bytes.as_slice_static())
    };

    store_wrapper.runtime.spawn(async move {
        let Some(path) = parse_path(&path_str, callback, user_data) else { return };

        let payload = PutPayload::from_bytes(bytes);

        let result = store
            .put(&path, payload)
            .await
            .map(drop)
            .map_err(|e| ErrorInfo::new(ErrorCode::ObjectStoreError, e));

        crate::invoke_callback(result, callback, user_data);
    });

    ErrorCode::Ok
}

/// Deletes an object from the in-memory store at the specified path.
///
/// # Safety
/// - `store_ptr` must be a valid pointer returned by `datafusion_in_memory_store_new`
/// - `path_ptr` must be a valid null-terminated C string representing the object path to delete
/// - `callback` will be invoked exactly once when the operation completes
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_in_memory_store_delete(
    store_ptr: *mut InMemoryStoreWrapper,
    path_ptr: *const std::ffi::c_char,
    callback: Callback,
    user_data: u64
) -> ErrorCode {
    let store_wrapper = ffi_ref!(store_ptr);
    let path_str = ffi_cstr_to_string!(path_ptr);

    debug!("Deleting data from in-memory store {store_ptr:p} at path '{path_str}'");

    let store = Arc::clone(&store_wrapper.inner);

    store_wrapper.runtime.spawn(async move {
        let Some(path) = parse_path(&path_str, callback, user_data) else { return };

        let result = store
            .delete(&path)
            .await
            .map_err(|e| ErrorInfo::new(ErrorCode::ObjectStoreError, e));

        crate::invoke_callback(result, callback, user_data);
    });

    ErrorCode::Ok
}

fn parse_path(path_str: &str, callback: Callback, user_data: u64) -> Option<Path> {
    match Path::parse(path_str) {
        Ok(path) => Some(path),
        Err(e) => {
            error!("Failed to parse path: {e}");
            crate::invoke_callback_error(
                &ErrorInfo::new(ErrorCode::InvalidArgument, e),
                callback,
                user_data,
            );
            None
        }
    }
}
