use log::{error, warn};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::ErrorCode;
use crate::error::ErrorInfo;

pub(crate) struct CancellationTokenGuard {
    token: CancellationToken,
    user_data: isize,
}

impl CancellationTokenGuard {
    pub(crate) fn new(user_data: isize) -> Self {
        Self {
            token: CancellationToken::new(),
            user_data,
        }
    }

    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub(crate) fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.token.cancelled()
    }
}

impl Drop for CancellationTokenGuard {
    fn drop(&mut self) {
        let mut tokens = CANCELLATION_TOKENS.lock().unwrap();
        tokens.remove(&self.user_data);
    }
}

static CANCELLATION_TOKENS: LazyLock<Mutex<HashMap<isize, CancellationToken>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn create_token(user_data: isize) -> CancellationTokenGuard {
    let token_guard = CancellationTokenGuard::new(user_data);

    let mut tokens = CANCELLATION_TOKENS.lock().unwrap();
    tokens.insert(user_data, token_guard.token());

    token_guard
}

pub(crate) fn error() -> ErrorInfo {
    ErrorInfo::new(ErrorCode::Canceled, "Operation was cancelled")
}

/// Cancels the operation associated with the given user data.
///
/// # Safety
/// - `user_data` must be a valid identifier for an ongoing operation that can be cancelled
///
/// # Returns
/// - `ErrorCode::Ok` if the operation was successfully cancelled
/// - `ErrorCode::InvalidArgument` if no operation is associated with the given user data
/// - `ErrorCode::Panic` if there was an error accessing the cancellation tokens
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_cancel_operation(user_data: isize) -> ErrorCode {
    let token: Option<CancellationToken>;
    {
        let Ok(mut tokens) = CANCELLATION_TOKENS.lock() else {
            error!("Failed to acquire lock on cancellation tokens");
            return ErrorCode::Panic;
        };
        token = tokens.remove(&user_data);
    }

    if let Some(token) = token {
        token.cancel();
        ErrorCode::Ok
    } else {
        warn!("No cancellation token found for user_data={user_data}, cannot cancel operation");
        ErrorCode::InvalidArgument
    }
}
