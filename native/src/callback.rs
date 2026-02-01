pub type Callback = extern "C" fn(
    result: *const std::ffi::c_void,
    error: *const ErrorInfoData,
    user_data: u64
);

#[repr(C)]
pub struct BytesData {
    data: *const u8,
    len: u32,
}

impl BytesData {
    pub(crate) fn new(s: &[u8]) -> Self {
        BytesData {
            data: s.as_ptr(),
            len: s.len() as u32,
        }
    }
}

#[repr(C)]
pub struct ErrorInfoData {
    pub code: crate::ErrorCode,
    pub message: BytesData
}

impl ErrorInfoData {
    fn new(err: &crate::ErrorInfo) -> Self {
        ErrorInfoData {
            code: err.code(),
            message: BytesData::new(err.message().as_bytes()),
        }
    }
}

pub(crate) fn invoke_callback<T>(result: Result<T, crate::ErrorInfo>, callback: Callback, user_data: u64) {
    match result {
        Ok(value) => {
            let value_ptr = &value as *const T as *const std::ffi::c_void;
            callback(value_ptr, std::ptr::null(), user_data);
        }
        Err(error) => {
            let err_info = ErrorInfoData::new(&error);
            let err_into_ptr = &err_info as *const ErrorInfoData;
            callback(std::ptr::null(), err_into_ptr, user_data);
        }
    }
}
