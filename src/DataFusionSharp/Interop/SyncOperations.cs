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
    
    public void PeekResult(ulong id)
    {
        if (!_operations.TryRemove(id, out var result) || result is not { } r)
            throw new DataFusionException(DataFusionErrorCode.Panic, "No result available for the given operation");
        
        if (r.Exception is not null)
            throw r.Exception;
    }

    private class SyncOperationResult
    {
        public Exception? Exception { get; set; }
    }
}