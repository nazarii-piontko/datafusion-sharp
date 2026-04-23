use log::{debug, error, warn};
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
    runtime_ptr: *mut *mut RuntimeHandle,
) -> crate::ErrorCode {
    if runtime_ptr.is_null() {
        error!("Failed to create runtime: invalid output pointer");
        return crate::ErrorCode::InvalidArgument;
    }

    debug!(
        "Creating Tokio runtime with worker_threads={worker_threads}, max_blocking_threads={max_blocking_threads}"
    );

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
            unsafe {
                *runtime_ptr = Box::into_raw(Box::new(runtime_handle));
            }

            debug!(
                "Created Tokio runtime with worker_threads={worker_threads}, max_blocking_threads={max_blocking_threads}, runtime_ptr={runtime_ptr:p}"
            );

            crate::ErrorCode::Ok
        }
        Err(err) => {
            error!("Failed to create runtime: {err}");
            crate::ErrorCode::RuntimeInitializationFailed
        }
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
pub unsafe extern "C" fn datafusion_runtime_destroy(
    runtime_ptr: *mut RuntimeHandle,
) -> crate::ErrorCode {
    if runtime_ptr.is_null() {
        warn!("Destroying runtime: null pointer, ignoring");
        return crate::ErrorCode::Ok;
    }

    let runtime_handle = unsafe { Box::from_raw(runtime_ptr) };

    debug!("Destroying runtime {runtime_ptr:p}");

    match Arc::try_unwrap(*runtime_handle) {
        Ok(runtime) => {
            runtime.shutdown_background();

            debug!("Destroyed runtime {runtime_ptr:p}");

            crate::ErrorCode::Ok
        }
        Err(arc) => {
            error!(
                "Failed to destroy runtime: {} strong references remain",
                Arc::strong_count(&arc)
            );

            crate::ErrorCode::RuntimeInitializationFailed
        }
    }
}

/// Example async function to test runtime functionality.
/// Sleeps for `timeout_ms` milliseconds, then invokes the callback with the result.
///
/// # Safety
/// - `runtime_ptr` must be a valid pointer to a runtime created by `datafusion_runtime_new`
/// - `callback` must be valid to call from any thread
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_ping(
    runtime_ptr: *mut RuntimeHandle,
    timeout_ms: u64,
    callback: crate::Callback,
    user_data: isize,
) -> crate::ErrorCode {
    let runtime = ffi_ref!(runtime_ptr);

    debug!("Received ping with timeout_ms={timeout_ms}, user_data={user_data}");

    let cancellation_guard = crate::cancellation::create_token(user_data);

    runtime.spawn(async move {
        debug!("Ping spawned with timeout_ms={timeout_ms}, user_data={user_data}");

        let result = tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_millis(timeout_ms)) => {
                debug!("Ping completed for user_data={user_data}");
                Ok(())
            },
            () = cancellation_guard.cancelled() => {
                debug!("Ping cancelled for user_data={user_data}");
                Err(crate::cancellation::error())
            }
        };

        crate::invoke_callback(result, callback, user_data);

        debug!("Ping finished with timeout_ms={timeout_ms}, user_data={user_data}");
    });

    crate::ErrorCode::Ok
}
