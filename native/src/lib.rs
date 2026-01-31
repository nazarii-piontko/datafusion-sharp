mod error;

use std::ffi::*;
use datafusion::prelude::{DataFrame, SessionContext};

pub use error::ErrorCode;

static RUNTIME: std::sync::RwLock<Option<tokio::runtime::Runtime>> = std::sync::RwLock::new(None);

fn with_runtime<F, T>(f: F) -> Result<T, ErrorCode>
where
    F: FnOnce(&tokio::runtime::Runtime) -> T,
{
    let guard = RUNTIME.read().map_err(|_| ErrorCode::LockPoisoned)?;
    let rt = guard.as_ref().ok_or(ErrorCode::NotInitialized)?;
    Ok(f(rt))
}

/// Wrapper around SessionContext with error state
pub struct Context {
    inner: SessionContext,
    last_error: Option<String>,
}

impl Context {
    fn new() -> Self {
        Self {
            inner: SessionContext::new(),
            last_error: None,
        }
    }

    fn set_error(&mut self, e: impl ToString) {
        self.last_error = Some(e.to_string());
    }

    fn clear_error(&mut self) {
        self.last_error = None;
    }
}

// =============================================================================
// Runtime
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_init(worker_threads: u32, max_blocking_threads: u32) -> ErrorCode {
    let Ok(mut guard) = RUNTIME.write() else {
        return ErrorCode::LockPoisoned;
    };

    if guard.is_some() {
        return ErrorCode::AlreadyInitialized;
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
            *guard = Some(runtime);
            ErrorCode::Ok
        }
        Err(_) => ErrorCode::RuntimeError,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_shutdown(timeout_millis: u64) -> ErrorCode {
    let Ok(mut guard) = RUNTIME.write() else {
        return ErrorCode::LockPoisoned;
    };

    if let Some(rt) = guard.take() {
        rt.shutdown_timeout(std::time::Duration::from_millis(timeout_millis));
    }

    ErrorCode::Ok
}

// =============================================================================
// Session Context
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_new() -> *mut Context {
    Box::into_raw(Box::new(Context::new()))
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_free(ctx: *mut Context) {
    if !ctx.is_null() {
        unsafe { drop(Box::from_raw(ctx)) };
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_last_error(ctx: *const Context) -> *const c_char {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let ctx = unsafe { &*ctx };
    ctx.last_error
        .as_ref()
        .map(|s| s.as_ptr() as *const c_char)
        .unwrap_or(std::ptr::null())
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_last_error_length(ctx: *const Context) -> i32 {
    if ctx.is_null() {
        return 0;
    }
    let ctx = unsafe { &*ctx };
    ctx.last_error.as_ref().map(|s| s.len() as i32).unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_register_csv(
    ctx: *mut Context,
    table_name: *const c_char,
    path: *const c_char,
) -> ErrorCode {
    if ctx.is_null() || table_name.is_null() || path.is_null() {
        return ErrorCode::NullPointer;
    }

    let ctx = unsafe { &mut *ctx };
    ctx.clear_error();

    let table_name = match unsafe { CStr::from_ptr(table_name) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            ctx.set_error(e);
            return ErrorCode::InvalidUtf8;
        }
    };

    let path = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            ctx.set_error(e);
            return ErrorCode::InvalidUtf8;
        }
    };

    let result = with_runtime(|rt| {
        rt.block_on(async {
            ctx.inner
                .register_csv(table_name, path, datafusion::prelude::CsvReadOptions::new())
                .await
        })
    });

    match result {
        Ok(Ok(())) => ErrorCode::Ok,
        Ok(Err(e)) => {
            ctx.set_error(e);
            ErrorCode::DataFusionError
        }
        Err(code) => code,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_context_sql(
    ctx: *mut Context,
    sql: *const c_char,
) -> *mut DataFrame {
    if ctx.is_null() || sql.is_null() {
        return std::ptr::null_mut();
    }

    let ctx = unsafe { &mut *ctx };
    ctx.clear_error();

    let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            ctx.set_error(e);
            return std::ptr::null_mut();
        }
    };

    let result = with_runtime(|rt| {
        rt.block_on(async { ctx.inner.sql(sql).await })
    });

    match result {
        Ok(Ok(df)) => Box::into_raw(Box::new(df)),
        Ok(Err(e)) => {
            ctx.set_error(e);
            std::ptr::null_mut()
        }
        Err(code) => {
            ctx.set_error(format!("Runtime error: {:?}", code));
            std::ptr::null_mut()
        }
    }
}

// =============================================================================
// DataFrame
// =============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_free(df: *mut DataFrame) {
    if !df.is_null() {
        unsafe { drop(Box::from_raw(df)) };
    }
}

/// Get the number of rows by collecting the dataframe
/// Note: This consumes the dataframe
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_count(
    ctx: *mut Context,
    df: *mut DataFrame,
    out_count: *mut u64,
) -> ErrorCode {
    if ctx.is_null() || df.is_null() || out_count.is_null() {
        return ErrorCode::NullPointer;
    }

    let ctx = unsafe { &mut *ctx };
    ctx.clear_error();

    let df = unsafe { Box::from_raw(df) };

    let result = with_runtime(|rt| {
        rt.block_on(async { df.count().await })
    });

    match result {
        Ok(Ok(count)) => {
            unsafe { *out_count = count as u64 };
            ErrorCode::Ok
        }
        Ok(Err(e)) => {
            ctx.set_error(e);
            ErrorCode::DataFusionError
        }
        Err(code) => code,
    }
}

/// Show dataframe (print to stdout for debugging)
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_show(
    ctx: *mut Context,
    df: *const DataFrame,
) -> ErrorCode {
    if ctx.is_null() || df.is_null() {
        return ErrorCode::NullPointer;
    }

    let ctx = unsafe { &mut *ctx };
    ctx.clear_error();

    let df = unsafe { &*df };

    let result = with_runtime(|rt| {
        rt.block_on(async { df.clone().show().await })
    });

    match result {
        Ok(Ok(())) => ErrorCode::Ok,
        Ok(Err(e)) => {
            ctx.set_error(e);
            ErrorCode::DataFusionError
        }
        Err(code) => code,
    }
}

#[cfg(test)]
mod tests {}
