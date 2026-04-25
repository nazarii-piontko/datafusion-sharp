use tokio_util::sync::CancellationToken;

use crate::error::ErrorInfo;
use crate::ErrorCode;

pub(crate) fn error() -> ErrorInfo {
    ErrorInfo::new(ErrorCode::Canceled, "Operation was cancelled")
}

pub(crate) fn into_raw_ptr(
    token: &CancellationToken,
    cancellation_token_out_ptr: *mut *mut CancellationToken,
) {
    if cancellation_token_out_ptr.is_null() {
        return;
    }

    let boxed_token = Box::new(token.clone());

    unsafe {
        *cancellation_token_out_ptr = Box::into_raw(boxed_token);
    }
}

/// Cancels the operation associated with the given cancellation token pointer.
///
/// # Safety
/// - `token_ptr` must be a valid pointer to a `CancellationToken` created by `to_out_ptr`
/// - The caller must ensure that the token pointer is not used after this function is called
/// - This function will take ownership of the token pointer and drop it
///
/// # Returns
/// - `ErrorCode::Ok` if the operation was successfully cancelled
/// - `ErrorCode::InvalidArgument` if the token pointer is null
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_cancellation_token_cancel(
    token_ptr: *mut CancellationToken,
) -> ErrorCode {
    if token_ptr.is_null() {
        return ErrorCode::InvalidArgument;
    }

    let token = unsafe { Box::from_raw(token_ptr) };

    token.cancel();

    ErrorCode::Ok
}

/// Destroys the cancellation token associated with the given pointer.
///
/// # Safety
/// - `token_ptr` must be a valid pointer to a `CancellationToken` created by `to_out_ptr`
/// - The caller must ensure that the token pointer is not used after this function is called
/// - This function will take ownership of the token pointer and drop it
///
/// # Returns
/// - `ErrorCode::Ok` if the operation was successfully cancelled
/// - `ErrorCode::InvalidArgument` if the token pointer is null
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_cancellation_token_destroy(
    token_ptr: *mut CancellationToken,
) -> ErrorCode {
    if token_ptr.is_null() {
        return ErrorCode::InvalidArgument;
    }

    unsafe { drop(Box::from_raw(token_ptr)) };

    ErrorCode::Ok
}
