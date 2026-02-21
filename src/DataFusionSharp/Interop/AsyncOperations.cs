using System.Collections.Concurrent;

namespace DataFusionSharp.Interop;

internal sealed class AsyncOperations
{
    public static AsyncOperations Instance { get; } = new();
    
    private sealed record Operation(object TaskCompletionSource, object? UserData);
    
    private readonly ConcurrentDictionary<ulong, object> _operations = new();
    private ulong _nextId;

    public (ulong Id, TaskCompletionSource TaskCompletionSource) Create() => Create<object>(null);

    public (ulong Id, TaskCompletionSource TaskCompletionSource) Create<TUserData>(TUserData? data)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.None);
        var op = new Operation(tcs, data);
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, op))
            id = Interlocked.Increment(ref _nextId);
        
        return (id, tcs);
    }
    
    public (ulong Id, TaskCompletionSource<TResult> Source) Create<TResult>() => Create<TResult, object>(null);
    
    public (ulong Id, TaskCompletionSource<TResult> Source) Create<TResult, TUserData>(TUserData? data)
    {
        var tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.None);
        var op = new Operation(tcs, data);
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, op))
            id = Interlocked.Increment(ref _nextId);
        
        return (id, tcs);
    }
    
    public TUserData? GetUserData<TUserData>(ulong id)
    {
        if (_operations.TryGetValue(id, out var op) && op is Operation { UserData: TUserData data })
            return data;
        return default;
    }

    public void Abort(ulong id)
    {
        _operations.TryRemove(id, out _);
    }
    
    public void CompleteVoid(ulong id, Exception? exception = null)
    {
        if (!_operations.TryRemove(id, out var t) || t is not Operation op)
            return;
        
        if (op.TaskCompletionSource is not TaskCompletionSource tcs)
            return;
        
        if (exception == null)
            tcs.TrySetResult();
        else
            tcs.TrySetException(exception);
    }
    
    public void CompleteWithError<TResult>(ulong id, Exception exception)
    {
        if (!_operations.TryRemove(id, out var t) || t is not Operation op)
            return;
        
        if (op.TaskCompletionSource is not TaskCompletionSource<TResult> tcs)
            return;
        
        tcs.TrySetException(exception);
    }
    
    public void CompleteWithResult<TResult>(ulong id, TResult result)
    {
        if (!_operations.TryRemove(id, out var t) || t is not Operation op)
            return;
        
        if (op.TaskCompletionSource is not TaskCompletionSource<TResult> tcs)
            return;
        
        tcs.TrySetResult(result);
    }
}