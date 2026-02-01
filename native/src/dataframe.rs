pub struct DataFrameWrapper {
    runtime: crate::RuntimeHandle,
    inner: datafusion::prelude::DataFrame,
}

impl DataFrameWrapper {
    pub fn new(runtime: crate::RuntimeHandle, inner: datafusion::prelude::DataFrame) -> Self {
        Self {
            runtime,
            inner,
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_destroy(dataframe_ptr: *mut DataFrameWrapper) -> crate::ErrorCode {
    if !dataframe_ptr.is_null() {
        unsafe { drop(Box::from_raw(dataframe_ptr)) };
    }

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_count(
    dataframe_ptr: *mut DataFrameWrapper,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    if dataframe_ptr.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let Some(callback) = callback else {
        return crate::ErrorCode::InvalidArgument;
    };

    let dataframe = unsafe { &*dataframe_ptr };
    let runtime = std::sync::Arc::clone(&dataframe.runtime);
    let inner = dataframe.inner.clone();

    dev_msg!("Executing count on DataFrame: {:p}", dataframe_ptr);

    runtime.spawn(async move {
        let result = inner
            .count()
            .await
            .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e))
            .map(|s| s as u64);

        crate::invoke_callback(result, callback, callback_user_data);
    });

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_show(
    df: *mut DataFrameWrapper,
    limit: u64,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    if df.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let Some(callback) = callback else {
        return crate::ErrorCode::InvalidArgument;
    };

    let dataframe = unsafe { &*df };
    let runtime = std::sync::Arc::clone(&dataframe.runtime);
    let inner = dataframe.inner.clone();

    dev_msg!("Executing show on DataFrame: {:p}", df);

    runtime.spawn(async move {
        let result = if limit > 0 {
            inner.show_limit(limit as usize).await
        } else {
            inner.show().await
        }.map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));

        crate::invoke_callback(result, callback, callback_user_data);
    });

    crate::ErrorCode::Ok
}

#[unsafe(no_mangle)]
pub extern "C" fn datafusion_dataframe_schema(
    df: *mut DataFrameWrapper,
    callback: Option<crate::Callback>,
    callback_user_data: u64
) -> crate::ErrorCode {
    if df.is_null() {
        return crate::ErrorCode::InvalidArgument;
    }

    let Some(callback) = callback else {
        return crate::ErrorCode::InvalidArgument;
    };

    let dataframe = unsafe { &*df };

    dev_msg!("Executing schema on DataFrame: {:p}", df);

    let schema = dataframe.inner.schema();

    let mut serialized_data = Vec::new();

    let result = datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut serialized_data, schema.as_arrow())
        .and_then(|mut s| s.flush())
        .map(|_| crate::callback::BytesData::new(serialized_data.as_slice()))
        .map_err(|e| crate::ErrorInfo::new(crate::ErrorCode::DataFrameError, e));
    
    dev_msg!("Finished executing schema on DataFrame: {:p}, schema size: {}", df, serialized_data.len());

    crate::invoke_callback(result, callback, callback_user_data);

    crate::ErrorCode::Ok
}