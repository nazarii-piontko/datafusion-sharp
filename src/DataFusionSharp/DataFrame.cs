using Apache.Arrow;
using Apache.Arrow.Ipc;
using DataFusionSharp.Interop;

namespace DataFusionSharp;

public class DataFrame : IDisposable
{
    private IntPtr _handle;
    
    public SessionContext Context { get; }
    
    internal DataFrame(SessionContext sessionContext, IntPtr handle)
    {
        Context = sessionContext;
        _handle = handle;
    }
    
    ~DataFrame()
    {
        DestroyDataFrame();
    }
    
    public Task<ulong> CountAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<ulong>();
        var result = NativeMethods.DataFrameCount(_handle, AsyncOperationGenericCallbacks.UInt64Result, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start counting rows in DataFrame");
        }
        return tcs.Task;
    }

    public Task ShowAsync(ulong? limit = null)
    {
        if (limit.HasValue)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit.Value, nameof(limit));
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameShow(_handle, limit ?? 0, AsyncOperationGenericCallbacks.VoidResult, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start showing DataFrame");
        }
        return tcs.Task;
    }
    
    public Task<string> ToStringAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<string>();
        var result = NativeMethods.DataFrameToString(_handle, AsyncOperationGenericCallbacks.StringResult, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start converting DataFrame to string");
        }
        return tcs.Task;
    }
    
    public Task<Schema> GetSchemaAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<Schema>();
        var result = NativeMethods.DataFrameSchema(_handle, CallbackForSchemaResult, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start getting DataFrame schema");
        }

        return tcs.Task;
    }
    
    public Task<CollectedData> CollectAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<CollectedData>();
        var result = NativeMethods.DataFrameCollect(_handle, CallbackForCollectResult, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start collecting DataFrame");
        }

        return tcs.Task;
    }
    
    public async Task<DataFrameStream> ExecuteStreamAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<IntPtr>();
        var result = NativeMethods.DataFrameExecuteStream(_handle, AsyncOperationGenericCallbacks.IntPtrResult, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start executing stream on DataFrame");
        }

        var streamHandle = await tcs.Task;
        return new DataFrameStream(this, streamHandle);
    }
    
    public void Dispose()
    {
        DestroyDataFrame();
        GC.SuppressFinalize(this);
    }
    
    private static void CallbackForSchemaResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            try
            {
                var data = BytesData.FromIntPtr(result);

                using var nativeMemoryManager = new NativeMemoryManager(data.DataPtr, data.Length);
                using var reader = new ArrowStreamReader(nativeMemoryManager.Memory);
                AsyncOperations.Instance.CompleteWithResult(handle, reader.Schema);
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<Schema>(handle, ex);
            }
        }
        else
            AsyncOperations.Instance.CompleteWithError<Schema>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    
    private static void CallbackForCollectResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            try
            {
                var data = BytesData.FromIntPtr(result);

                using var nativeMemoryManager = new NativeMemoryManager(data.DataPtr, data.Length);
                using var reader = new ArrowStreamReader(nativeMemoryManager.Memory);
                
                var batches = new List<RecordBatch>();
                while (reader.ReadNextRecordBatch() is {} batch)
                    batches.Add(batch);
                
                AsyncOperations.Instance.CompleteWithResult(handle, new CollectedData(batches, reader.Schema));
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<CollectedData>(handle, ex);
            }
        }
        else
            AsyncOperations.Instance.CompleteWithError<CollectedData>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    
    private void DestroyDataFrame()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;
        
        _handle = IntPtr.Zero;
        
        NativeMethods.DataFrameDestroy(handle);
    }
    
    public record CollectedData(List<RecordBatch> Batches, Schema Schema);
}