use log::{LevelFilter, Log, Metadata, Record};

use crate::{BytesData, ErrorCode};

pub type LogCallback = unsafe extern "C" fn(
    level: u32,
    target: BytesData,
    message: BytesData,
);

struct DataFusionLogger {
    callback: LogCallback,
}

impl Log for DataFusionLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let target_bytes = BytesData::new(record.target().as_bytes());
        let message = record.args().to_string();
        let message_bytes = BytesData::new(message.as_bytes());

        unsafe { (self.callback)(record.level() as u32, target_bytes, message_bytes); }
    }

    fn flush(&self) {}
}

/// Registers the global logger with a callback that forwards log messages via FFI.
///
/// The initial max level is set to `Trace` (most permissive). Use
/// `datafusion_set_log_level` to change the level filter at any time.
///
/// # Safety
/// - `callback` must be a valid function pointer that remains valid for the lifetime of the program
/// - `callback` must be safe to call from any thread
/// - This function must be called at most once; subsequent calls will return an error
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_set_logger(callback: LogCallback) -> ErrorCode {
    log::set_max_level(LevelFilter::Trace);

    match log::set_boxed_logger(Box::new(DataFusionLogger { callback })) {
        Ok(()) => ErrorCode::Ok,
        Err(_) => ErrorCode::RuntimeInitializationFailed,
    }
}

/// Sets the global log level filter.
///
/// This can be called at any time to change the maximum log level.
///
/// # Arguments
/// - `min_level`: 0=Off, 1=Error, 2=Warn, 3=Info, 4=Debug, 5=Trace
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_set_log_level(max_level: u32) -> ErrorCode {
    let level_filter = match max_level {
        0 => LevelFilter::Off,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        5 => LevelFilter::Trace,
        _ => return ErrorCode::InvalidArgument,
    };

    log::set_max_level(level_filter);
    ErrorCode::Ok
}
