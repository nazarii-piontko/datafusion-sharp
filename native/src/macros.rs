#[cfg(debug_assertions)]
#[macro_export]
macro_rules! dev_msg {
    ($($arg:tt)*) => {
        eprintln!("[datafusion-sharp-native]({}) ({}:{}) {}",
            chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, false),
            file!(),
            line!(),
            format!($($arg)*));
    };
}

#[cfg(not(debug_assertions))]
macro_rules! dev_msg {
    ($($arg:tt)*) => {};
}

/// Converts a raw pointer to a reference.
/// Returns `InvalidArgument` error code if the pointer is null.
#[macro_export]
macro_rules! ffi_ref {
    ($ptr:expr) => {{
        if $ptr.is_null() {
            return $crate::ErrorCode::InvalidArgument;
        }
        unsafe { &*$ptr }
    }};
}

/// Converts a raw optional pointer to a reference.
#[macro_export]
macro_rules! ffi_opt_ref {
    ($ptr:expr) => {{
        unsafe { $ptr.as_ref() }
    }};
}

/// Converts a raw pointer to a mutable reference.
/// Returns `InvalidArgument` error code if the pointer is null.
#[macro_export]
macro_rules! ffi_ref_mut {
    ($ptr:expr) => {{
        if $ptr.is_null() {
            return $crate::ErrorCode::InvalidArgument;
        }
        unsafe { &mut *$ptr }
    }};
}

/// Converts a C string pointer to an owned String.
/// Returns `InvalidArgument` error code if the pointer is null or not valid UTF-8.
#[macro_export]
macro_rules! ffi_cstr_to_string {
    ($ptr:expr) => {{
        if $ptr.is_null() {
            return $crate::ErrorCode::InvalidArgument;
        }
        let Ok(s) = unsafe { std::ffi::CStr::from_ptr($ptr) }
            .to_str()
            .map(|s| s.to_string())
        else {
            return $crate::ErrorCode::InvalidArgument;
        };
        s
    }};
}
