using System.Collections.Concurrent;

namespace DataFusionSharp.Interop;

internal sealed class AsyncOperations
{
    public static AsyncOperations Instance { get; } = new();

    private abstract class Operation
    {
        public ulong Id { get; set; }
        
        public object? UserData { get; init; }
        
        public CancellationTokenRegistration CancellationTokenCallbackRegistration { get; set; }

        public abstract void Cancel();

        public void UnregisterCancellationCallback()
        {
            CancellationTokenCallbackRegistration.Unregister();
        }
    }
    
    private sealed class Operation<TResult> : Operation
    {
        public required TaskCompletionSource<TResult> TaskCompletionSource { get; init; }
        
        public override void Cancel()
        {
            TaskCompletionSource.TrySetCanceled();
        }
    }

    private sealed class OperationVoid : Operation
    {
        public required TaskCompletionSource TaskCompletionSource { get; init; }
        
        public override void Cancel()
        {
            TaskCompletionSource.TrySetCanceled();
        }
    }
    
    private readonly ConcurrentDictionary<ulong, Operation> _operations = new();
    private ulong _nextId;

    public (ulong Id, TaskCompletionSource TaskCompletionSource) Create(CancellationToken cancellationToken = default)
    {
        return Create<object>(null, cancellationToken);
    }

    public (ulong Id, TaskCompletionSource TaskCompletionSource) Create<TUserData>(TUserData? data, CancellationToken cancellationToken = default)
    {
        // Create TaskCompletionSource with continuations running asynchronously to avoid potential deadlocks
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var op = new OperationVoid
        {
            TaskCompletionSource = tcs,
            UserData = data
        };
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, op))
            id = Interlocked.Increment(ref _nextId);
        
        op.Id = id;
        // Create registration after adding to dictionary to avoid race condition where cancellation could occur before the operation is added
        op.CancellationTokenCallbackRegistration = cancellationToken.Register(OnOperationCancelled, op);
        
        return (id, tcs);
    }
    
    public (ulong Id, TaskCompletionSource<TResult> Source) Create<TResult>(CancellationToken cancellationToken = default)
    {
        return Create<TResult, object>(null, cancellationToken);
    }

    public (ulong Id, TaskCompletionSource<TResult> Source) Create<TResult, TUserData>(TUserData? data, CancellationToken cancellationToken = default)
    {
        // Create TaskCompletionSource with continuations running asynchronously to avoid potential deadlocks
        var tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var op = new Operation<TResult>
        {
            TaskCompletionSource = tcs,
            UserData = data
        };
        
        var id = Interlocked.Increment(ref _nextId);
        while (!_operations.TryAdd(id, op))
            id = Interlocked.Increment(ref _nextId);

        op.Id = id;
        // Create registration after adding to dictionary to avoid race condition where cancellation could occur before the operation is added
        op.CancellationTokenCallbackRegistration = cancellationToken.Register(OnOperationCancelled, op);
        
        return (id, tcs);
    }
    
    public TUserData? GetUserData<TUserData>(ulong id)
    {
        if (_operations.TryGetValue(id, out var op) && op is { UserData: TUserData data })
            return data;
        return default;
    }
    
    public void EnsureNativeCall(ulong id, DataFusionErrorCode result, string errorMessage, CancellationToken cancellationToken)
    {
        if (result != DataFusionErrorCode.Ok)
        {
            Abort(id);
            cancellationToken.ThrowIfCancellationRequested();
            throw new DataFusionException(result, errorMessage);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            Cancel(id);
            cancellationToken.ThrowIfCancellationRequested();
        }
    }
    
    public void CompleteVoid(ulong id, Exception? exception = null)
    {
        if (!_operations.TryRemove(id, out var t) || t is not OperationVoid op)
            return;
        
        op.UnregisterCancellationCallback();

        switch (exception)
        {
            case null:
                op.TaskCompletionSource.TrySetResult();
                break;
            case DataFusionException { ErrorCode: DataFusionErrorCode.Canceled }:
                op.TaskCompletionSource.TrySetCanceled();
                break;
            default:
                op.TaskCompletionSource.TrySetException(exception);
                break;
        }
    }
    
    public void CompleteWithError<TResult>(ulong id, Exception exception)
    {
        if (!_operations.TryRemove(id, out var t) || t is not Operation<TResult> op)
            return;
        
        op.UnregisterCancellationCallback();
        
        if (exception is DataFusionException { ErrorCode: DataFusionErrorCode.Canceled })
            op.TaskCompletionSource.TrySetCanceled();
        else
            op.TaskCompletionSource.TrySetException(exception);
    }
    
    public void CompleteWithResult<TResult>(ulong id, TResult result)
    {
        if (!_operations.TryRemove(id, out var t) || t is not Operation<TResult> op)
            return;
        
        op.UnregisterCancellationCallback();
        
        op.TaskCompletionSource.TrySetResult(result);
    }
    
    private void Abort(ulong id)
    {
        if (_operations.TryRemove(id, out var op))
            op.UnregisterCancellationCallback();
    }

    private void Cancel(ulong id)
    {
        if (!_operations.TryRemove(id, out var op))
            return;

        NativeMethods.CancelOperation(op.Id);
        op.UnregisterCancellationCallback();
    }

    private static void OnOperationCancelled(object? obj)
    {
        if (obj is not Operation op)
            return;

        var result = NativeMethods.CancelOperation(op.Id);
            
        // If result is OK the native code will inform us when the operation is cancelled, so we don't need to do anything here.
        // If it's not OK, it means the operation has already completed or is in the process of completing, so we can just remove it.
        if (result == DataFusionErrorCode.Ok ||
            !Instance._operations.TryRemove(op.Id, out _))
            return;
        
        op.UnregisterCancellationCallback();
        op.Cancel();
    }
}