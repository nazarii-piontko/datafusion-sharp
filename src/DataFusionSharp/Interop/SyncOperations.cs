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
    
    public void Abort(ulong id)
    {
        _operations.TryRemove(id, out _);
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
    
    public void TakeResult(ulong id)
    {
        if (!_operations.TryRemove(id, out var result) || result is not { } r)
            throw new DataFusionException(DataFusionErrorCode.Panic, "No result available for the given operation");
        
        if (r.Exception is not null)
            throw r.Exception;
    }
    
    public TResult TakeResult<TResult>(ulong id)
    {
        if (!_operations.TryRemove(id, out var result) || result is not SyncOperationResult<TResult> r)
            throw new DataFusionException(DataFusionErrorCode.Panic, "No result available for the given operation");
        
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