using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static partial class NativeMethods
{
    private const string LibraryName = "datafusion_sharp_native";
    
    // Callback
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void Callback(IntPtr result, IntPtr error, ulong handle);
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void LogCallback(NativeLogLevel level, BytesData target, BytesData message);
    
    internal enum NativeLogLevel : uint
    {
        None = 0,
        Error,
        Warn,
        Info,
        Debug,
        Trace
    }
    
    // Logger

    [LibraryImport(LibraryName, EntryPoint = "datafusion_set_logger")]
    public static partial DataFusionErrorCode SetLogger(IntPtr callback);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_set_log_level")]
    public static partial DataFusionErrorCode SetLogLevel(NativeLogLevel maxLevel);

    // Runtime

    [LibraryImport(LibraryName, EntryPoint = "datafusion_runtime_new")]
    public static partial DataFusionErrorCode RuntimeNew(uint workerThreads, uint maxBlockingThreads, out IntPtr runtimeHandle);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_runtime_destroy")]
    public static partial DataFusionErrorCode RuntimeDestroy(IntPtr runtimeHandle);
    
    // Context
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_new")]
    public static partial DataFusionErrorCode ContextNew(RuntimeSafeHandle runtimeHandle, out IntPtr contextHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_destroy")]
    public static partial DataFusionErrorCode ContextDestroy(IntPtr contextHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_csv")]
    public static partial DataFusionErrorCode ContextRegisterCsv(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName, [MarshalAs(UnmanagedType.LPUTF8Str)] string filePath, BytesData optionsData, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_json")]
    public static partial DataFusionErrorCode ContextRegisterJson(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName, [MarshalAs(UnmanagedType.LPUTF8Str)] string filePath, BytesData optionsData, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_parquet")]
    public static partial DataFusionErrorCode ContextRegisterParquet(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName, [MarshalAs(UnmanagedType.LPUTF8Str)] string filePath, BytesData optionsData, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_batch")]
    public static partial DataFusionErrorCode ContextRegisterBatch(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName, BytesData batchIpcData, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_deregister_table")]
    public static partial DataFusionErrorCode ContextDeregisterTable(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_sql")]
    public static partial DataFusionErrorCode ContextSql(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string sql, BytesData paramValuesData, IntPtr callback, ulong userData);

    // Object Store

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_object_store_local")]
    public static partial DataFusionErrorCode ContextRegisterObjectStoreLocal(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string url, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_object_store_s3")]
    public static partial DataFusionErrorCode ContextRegisterObjectStoreS3(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string url, BytesData optionsData, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_object_store_azure")]
    public static partial DataFusionErrorCode ContextRegisterObjectStoreAzure(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string url, BytesData optionsData, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_object_store_gcs")]
    public static partial DataFusionErrorCode ContextRegisterObjectStoreGcs(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string url, BytesData optionsData, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_object_store_http")]
    public static partial DataFusionErrorCode ContextRegisterObjectStoreHttp(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string url, BytesData optionsData, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_deregister_object_store")]
    public static partial DataFusionErrorCode ContextDeregisterObjectStore(SessionContextSafeHandle contextHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string url, IntPtr callback, ulong userData);

    // DataFrame
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_destroy")]
    public static partial DataFusionErrorCode DataFrameDestroy(IntPtr dataFrameHandle);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_clone")]
    public static partial IntPtr DataFrameClone(DataFrameSafeHandle dataFrameHandle);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_with_parameters")]
    public static partial DataFusionErrorCode DataFrameWithParameters(DataFrameSafeHandle dataFrameHandle, BytesData paramValuesData, IntPtr callback, ulong userData); 
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_count")]
    public static partial DataFusionErrorCode DataFrameCount(DataFrameSafeHandle dataFrameHandle, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_show")]
    public static partial DataFusionErrorCode DataFrameShow(DataFrameSafeHandle dataFrameHandle, ulong limit, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_schema")]
    public static partial DataFusionErrorCode DataFrameSchema(DataFrameSafeHandle dataFrameHandle, IntPtr callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_collect")]
    public static partial DataFusionErrorCode DataFrameCollect(DataFrameSafeHandle dataFrameHandle, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_to_string")]
    public static partial DataFusionErrorCode DataFrameToString(DataFrameSafeHandle dataFrameHandle, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_write_csv")]
    public static partial DataFusionErrorCode DataFrameWriteCsv(DataFrameSafeHandle dataFrameHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string path, BytesData dataFrameWriteOptionsData, BytesData csvWriteOptionsData, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_write_json")]
    public static partial DataFusionErrorCode DataFrameWriteJson(DataFrameSafeHandle dataFrameHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string path, BytesData dataFrameWriteOptionsData, BytesData jsonWriteOptionsData, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_write_parquet")]
    public static partial DataFusionErrorCode DataFrameWriteParquet(DataFrameSafeHandle dataFrameHandle, [MarshalAs(UnmanagedType.LPUTF8Str)] string path, BytesData dataFrameWriteOptionsData, BytesData parquetWriteOptionsData, IntPtr callback, ulong userData);

    // Stream

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_execute_stream")]
    public static partial DataFusionErrorCode DataFrameExecuteStream(DataFrameSafeHandle dataFrameHandle, IntPtr callback, ulong userData);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_stream_destroy")]
    public static partial DataFusionErrorCode DataFrameStreamDestroy(IntPtr streamHandle);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_stream_next")]
    public static partial DataFusionErrorCode DataFrameStreamNext(DataFrameStreamSafeHandle streamHandle, IntPtr callback, ulong userData);
}
