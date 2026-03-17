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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn datafusion_configure_logger(callback: LogCallback, min_level: u32) -> ErrorCode {
    let min_level = unsafe { std::mem::transmute::<usize, LevelFilter>(min_level as usize) };

    log::set_max_level(min_level);

    match log::set_boxed_logger(Box::new(DataFusionLogger { callback, min_level })) {
        Ok(_) => ErrorCode::Ok,
        Err(_) => ErrorCode::RuntimeInitializationFailed,
    }
}
