pub type Callback = unsafe extern "C" fn(
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
            #[allow(clippy::cast_possible_truncation)]
            len: s.len() as u32,
        }
    }

    pub(crate) fn as_opt_slice(&self) -> Option<&[u8]> {
        if self.data.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts( self.data, self.len as usize) })
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
        Ok(value) => invoke_callback_success(value, callback, user_data),
        Err(error) => invoke_callback_error(&error, callback, user_data)
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn invoke_callback_success<T>(result: T, callback: Callback, user_data: u64) {
    let value_ptr = (&raw const result).cast::<std::ffi::c_void>();
    unsafe { callback(value_ptr, std::ptr::null(), user_data); }
}

pub(crate) fn invoke_callback_error(error: &crate::ErrorInfo, callback: Callback, user_data: u64) {
    let err_info = ErrorInfoData::new(error);
    let err_into_ptr = &raw const err_info;
    unsafe { callback(std::ptr::null(), err_into_ptr, user_data); }
}

pub(crate) fn invoke_callback_null_result(callback: Callback, user_data: u64) {
    unsafe { callback(std::ptr::null(), std::ptr::null(), user_data); }
}
