use std::sync::Arc;

pub type RuntimeHandle = Arc<tokio::runtime::Runtime>;

/// Creates a new Tokio multithreaded runtime for `DataFusion`.
///
/// # Safety
/// - `runtime` must be a valid, aligned, non-null pointer to writable memory
/// - Caller must call `datafusion_runtime_destroy` exactly once with the returned pointer
///
/// # Parameters
/// - `worker_threads`: Number of worker threads (0 = automatic)
/// - `max_blocking_threads`: Max blocking threads (0 = automatic)
/// - `runtime`: Output pointer to receive the runtime pointer
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_runtime_new(
    worker_threads: u32,
    max_blocking_threads: u32,
    runtime_ptr: *mut *mut RuntimeHandle) -> crate::ErrorCode {
    if runtime_ptr.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let mut builder = tokio::runtime::Builder::new_multi_thread();

    if worker_threads > 0 {
        builder.worker_threads(worker_threads as usize);
    }

    if max_blocking_threads > 0 {
        builder.max_blocking_threads(max_blocking_threads as usize);
    }

    builder.enable_all();

    match builder.build() {
        Ok(runtime) => {
            let runtime_handle: RuntimeHandle = Arc::new(runtime);
            unsafe { *runtime_ptr = Box::into_raw(Box::new(runtime_handle)); }

            dev_msg!("Successfully created Tokio runtime: {:p}", unsafe { *runtime_ptr });

            crate::ErrorCode::Ok
        }
        Err(err) => {
            dev_msg!("Error creating Tokio runtime: {}", err);
            eprintln!("[datafusion-sharp-native] Failed to initialize Tokio runtime: {err}");
            crate::ErrorCode::RuntimeInitializationFailed
        },
    }
}

/// Destroys a Tokio runtime created by `datafusion_runtime_new`.
///
/// Shuts down the runtime, waiting up to `timeout_millis` milliseconds for tasks to complete.
///
/// # Safety
/// - `runtime` must be a valid pointer returned by `datafusion_runtime_new`
/// - Caller must not use `runtime` after this call
///
/// # Parameters
/// - `runtime`: Pointer to the runtime to destroy
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_runtime_destroy(runtime_ptr: *mut RuntimeHandle) -> crate::ErrorCode {
    if runtime_ptr.is_null() {
        return crate::ErrorCode::Ok;
    }

    dev_msg!("Destroying Tokio runtime: {:p}", runtime_ptr);

    let runtime_handle = unsafe { Box::from_raw(runtime_ptr) };

    dev_msg!("Attempting to destroy Tokio runtime: {:p}", runtime_ptr);

    match Arc::try_unwrap(*runtime_handle) {
        Ok(runtime) => {
            runtime.shutdown_background();

            dev_msg!("Successfully dropped Tokio runtime: {:p}", runtime_ptr);

            crate::ErrorCode::Ok
        }
        Err(arc) => {
            dev_msg!("Cannot destroy Tokio runtime due to strong references: {:p}, references: {}", runtime_ptr, Arc::strong_count(&arc));

            eprintln!("[datafusion-sharp-native] Cannot destroy Tokio runtime: there are still {} strong references", Arc::strong_count(&arc));

            crate::ErrorCode::RuntimeInitializationFailed
        }
    }
}