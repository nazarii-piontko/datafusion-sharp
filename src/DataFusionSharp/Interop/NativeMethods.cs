using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static partial class NativeMethods
{
    private const string LibraryName = "datafusion_sharp_native";

    // =========================================================================
    // Runtime
    // =========================================================================

    [LibraryImport(LibraryName, EntryPoint = "datafusion_init")]
    public static partial ErrorCode Init(uint workerThreads, uint maxBlockingThreads);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_shutdown")]
    public static partial ErrorCode Shutdown(ulong timeoutMillis);

    // =========================================================================
    // Session Context
    // =========================================================================

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_new")]
    public static partial IntPtr ContextNew();

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_free")]
    public static partial void ContextFree(IntPtr ctx);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_last_error")]
    public static partial IntPtr ContextLastError(IntPtr ctx);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_last_error_length")]
    public static partial int ContextLastErrorLength(IntPtr ctx);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_register_csv", StringMarshalling = StringMarshalling.Utf8)]
    public static partial ErrorCode ContextRegisterCsv(IntPtr ctx, string tableName, string path);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_context_sql", StringMarshalling = StringMarshalling.Utf8)]
    public static partial IntPtr ContextSql(IntPtr ctx, string sql);

    // =========================================================================
    // DataFrame
    // =========================================================================

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_free")]
    public static partial void DataFrameFree(IntPtr df);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_count")]
    public static partial ErrorCode DataFrameCount(IntPtr ctx, IntPtr df, out ulong count);

    [LibraryImport(LibraryName, EntryPoint = "datafusion_dataframe_show")]
    public static partial ErrorCode DataFrameShow(IntPtr ctx, IntPtr df);
}
