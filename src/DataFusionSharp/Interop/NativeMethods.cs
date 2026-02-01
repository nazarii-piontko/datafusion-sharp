using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static partial class NativeMethods
{
    private const string LibraryName = "datafusion_sharp_native";

    // Runtime

    [LibraryImport(LibraryName, EntryPoint = "datafusion_runtime_new")]
    public static partial DataFusionErrorCode RuntimeNew(uint workerThreads, uint maxBlockingThreads, out IntPtr runtimeHandle);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_runtime_destroy")]
    public static partial DataFusionErrorCode RuntimeShutdown(IntPtr runtimeHandle, ulong timeoutMillis);
    
    // Context
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_new")]
    public static partial DataFusionErrorCode ContextNew(IntPtr runtimeHandle, out IntPtr contextHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_destroy")]
    public static partial DataFusionErrorCode ContextDestroy(IntPtr contextHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_csv")]
    public static partial DataFusionErrorCode ContextRegisterCsv(IntPtr contextHandle, [MarshalAs(UnmanagedType.LPStr)] string tableName, [MarshalAs(UnmanagedType.LPStr)] string filePath, AsyncCallback callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_sql")]
    public static partial DataFusionErrorCode ContextSql(IntPtr contextHandle, [MarshalAs(UnmanagedType.LPStr)] string sql, AsyncCallback callback, ulong userData);
    
    // DataFrame
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_destroy")]
    public static partial DataFusionErrorCode DataFrameDestroy(IntPtr dataFrameHandle);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_count")]
    public static partial DataFusionErrorCode DataFrameCount(IntPtr dataFrameHandle, AsyncCallback callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_show")]
    public static partial DataFusionErrorCode DataFrameShow(IntPtr dataFrameHandle, ulong limit, AsyncCallback callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_schema")]
    public static partial DataFusionErrorCode DataFrameSchema(IntPtr dataFrameHandle, AsyncCallback callback, ulong userData);
    
    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_collect")]
    public static partial DataFusionErrorCode DataFrameCollect(IntPtr dataFrameHandle, AsyncCallback callback, ulong userData);
}