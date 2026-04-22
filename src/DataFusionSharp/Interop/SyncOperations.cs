using System.Collections.Concurrent;

namespace DataFusionSharp.Interop;

internal sealed class SyncOperations
{
    public static SyncOperations Instance { get; } = new();
    
    private readonly ConcurrentDictionary<ulong, SyncOperationResult?> _operations = new();
    private ulong _nextId;

    public ulong Create()
    {
        return Interlocked.Increment(ref _nextId);
    }
    
    public void CompleteVoid(ulong id, Exception? exception)
    {
        _operations.TryAdd(id, new SyncOperationResult { Exception = exception });
    }
    
    public void CompleteWithResult<TResult>(ulong id, TResult? result = default)
    {
        _operations.TryAdd(id, new SyncOperationResult<TResult> { Result = result });
    }
    
    public void CompleteWithError<TResult>(ulong id, Exception exception)
    {
        _operations.TryAdd(id, new SyncOperationResult<TResult> { Exception = exception });
    }
    
    public void TakeResult(ulong id, DataFusionErrorCode result, string errorMessage)
    {
        if (!_operations.TryRemove(id, out var op) || op is not { } r)
            throw new DataFusionException(DataFusionErrorCode.Panic, "No result available for the given operation");
        
        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, errorMessage);
        
        if (r.Exception is not null)
            throw r.Exception;
    }
    
    public TResult TakeResult<TResult>(ulong id,DataFusionErrorCode result, string errorMessage)
    {
        if (!_operations.TryRemove(id, out var op) || op is not SyncOperationResult<TResult> r)
            throw new DataFusionException(DataFusionErrorCode.Panic, "No result available for the given operation");
        
        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, errorMessage);
        
        if (r.Exception is not null)
            throw r.Exception;
        
        return r.Result!;
    }

    private class SyncOperationResult
    {
        public Exception? Exception { get; set; }
    }
    
    private sealed class SyncOperationResult<TResult> : SyncOperationResult
    {
        public TResult? Result { get; set; }
    }
}