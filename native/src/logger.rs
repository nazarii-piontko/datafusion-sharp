use log::{LevelFilter, Log, Metadata, Record};

use crate::{BytesData, ErrorCode};

pub type LogCallback = unsafe extern "C" fn(
    level: u32,
    target: BytesData,
    message: BytesData,
);

struct DataFusionLogger {
    callback: LogCallback,
    min_level: LevelFilter,
}

impl Log for DataFusionLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level().to_level_filter() <= self.min_level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let target_bytes = BytesData::new(record.target().as_bytes());
        let message = format!("{}", record.args());
        let message_bytes = BytesData::new(message.as_bytes());

        unsafe { (self.callback)(record.level() as u32, target_bytes, message_bytes); }
    }

    fn flush(&self) {}
}

/// Configures the global logger with a callback that forwards log messages via FFI.
///
/// # Safety
/// - `callback` must be a valid function pointer that remains valid for the lifetime of the program
/// - `callback` must be safe to call from any thread
/// - This function must be called at most once; subsequent calls will return an error
#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_configure_logger(callback: LogCallback, min_level: u32) -> ErrorCode {
    let min_level = match min_level {
        0 => LevelFilter::Off,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        5 => LevelFilter::Trace,
        _ => return ErrorCode::InvalidArgument,
    };

    log::set_max_level(min_level);

    match log::set_boxed_logger(Box::new(DataFusionLogger { callback, min_level })) {
        Ok(()) => ErrorCode::Ok,
        Err(_) => ErrorCode::RuntimeInitializationFailed,
    }
}
