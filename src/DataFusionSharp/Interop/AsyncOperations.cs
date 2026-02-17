using System.Collections.Concurrent;

namespace DataFusionSharp.Interop;

internal sealed class AsyncOperations
{
    public static AsyncOperations Instance { get; } = new();
    
    private readonly ConcurrentDictionary<ulong, object> _operations = new();
    private ulong _nextId;
    
    public (ulong Id, TaskCompletionSource TaskCompletionSource) Create()
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, tcs))
            id = Interlocked.Increment(ref _nextId);
        
        return (id, tcs);
    }
    
    public (ulong Id, TaskCompletionSource<T> Source) Create<T>()
    {
        var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, tcs))
            id = Interlocked.Increment(ref _nextId);
        
        return (id, tcs);
    }

    public void Abort(ulong id)
    {
        _operations.TryRemove(id, out _);
    }
    
    public void CompleteVoid(ulong id, Exception? exception = null)
    {
        if (!_operations.TryRemove(id, out var t))
            return;
        
        if (t is not TaskCompletionSource tcs)
            return;
        
        if (exception == null)
            tcs.TrySetResult();
        else
            tcs.TrySetException(exception);
    }
    
    public void CompleteWithError<T>(ulong id, Exception exception)
    {
        if (!_operations.TryRemove(id, out var t))
            return;
        
        if (t is not TaskCompletionSource<T> tcs)
            return;
        
        tcs.TrySetException(exception);
    }
    
    public void CompleteWithResult<T>(ulong id, T result)
    {
        if (!_operations.TryRemove(id, out var t))
            return;
        
        if (t is not TaskCompletionSource<T> tcs)
            return;
        
        tcs.TrySetResult(result);
    }
}