#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Ok = 0,
    Panic = 1,
    InvalidArgument = 2,
    RuntimeInitializationFailed = 3,
    RuntimeShutdownFailed = 4,
    TableRegistrationFailed = 5,
    SqlError = 6,
    DataFrameError = 7
}

#[derive(Debug, Clone)]
pub(crate) struct ErrorInfo {
    code: ErrorCode,
    message: String
}

impl ErrorInfo {
    pub fn new<E: std::fmt::Display>(code: ErrorCode, error: E) -> Self {
        Self {
            code,
            message: error.to_string()
        }
    }
    
    pub fn code(&self) -> ErrorCode {
        self.code
    }
    
    pub fn message(&self) -> &str {
        &self.message
    }
}
