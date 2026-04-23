using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal abstract class AsyncOperation
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
#endif

    private readonly CancellationToken _cancellationToken;
    private readonly CancellationTokenRegistration _cancellationRegistration;
    
    private GCHandle _handle;
    
    protected AsyncOperation(CancellationToken cancellationToken)
    {
        _cancellationToken = cancellationToken;
        _cancellationRegistration = cancellationToken.Register(OnCancelled);
    }

    internal IntPtr GetHandle()
    {
        if (!_handle.IsAllocated)
        {
            _handle = GCHandle.Alloc(this, GCHandleType.Normal);
#if MEMORY_TEST
            Interlocked.Increment(ref _liveInstances);
#endif
        }

        return GCHandle.ToIntPtr(_handle);
    }
    
    internal void EnsureNativeCall(DataFusionErrorCode result, string errorMessage)
    {
        if (result != DataFusionErrorCode.Ok)
        {
            // Native call failed, the operation has not been started.
            // We can just clean up.
            Cleanup();
            _cancellationToken.ThrowIfCancellationRequested();
            throw new DataFusionException(result, errorMessage);
        }

        if (_cancellationToken.IsCancellationRequested)
        {
            // The operation has been started, but the token was already cancelled.
            // We need forcibly to attempt to cancel the operation on the native side.
            // If the token was cancelled before native code registered async operation,
            // native code will not have the chance to actually do cancellation.
            if (_handle.IsAllocated)
            {
                var cancelResult = NativeMethods.CancelOperation(GCHandle.ToIntPtr(_handle));
                if (cancelResult != DataFusionErrorCode.Ok)
                {
                    // Cancellation fails, it means the operation has already cancelled or completed.
                    // We can just clean up.
                    Cleanup();
                    _cancellationToken.ThrowIfCancellationRequested();
                }
            }
            else
            {
                // Should never happen.
                Debug.Assert(false, "Expected handle to be allocated when cancellation is requested");
                
                // We can just clean up.
                Cleanup();
                _cancellationToken.ThrowIfCancellationRequested();
            }
        }
    }
    
    protected void Cleanup()
    {
        if (_handle.IsAllocated)
        {
            try
            {
                _handle.Free();
#if MEMORY_TEST
                Interlocked.Decrement(ref _liveInstances);
#endif
            }
            catch (InvalidOperationException)
            {
                // Handle was already freed, ignore
            }
        }

        try
        {
            _cancellationRegistration.Unregister();
        }
        catch (ObjectDisposedException)
        {
            // CancellationTokenSource was already disposed, ignore
        }
    }

    private void OnCancelled()
    {
        if (_handle.IsAllocated)
            NativeMethods.CancelOperation(GCHandle.ToIntPtr(_handle));
    }
}

internal sealed class AsyncVoidOperation : AsyncOperation
{
    private readonly TaskCompletionSource _taskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
    
    internal Task Task => _taskCompletionSource.Task;
    
    internal AsyncVoidOperation(CancellationToken cancellationToken)
        : base(cancellationToken)
    {
    }

    internal void Complete(Exception? exception = null)
    {
        Cleanup();

        switch (exception)
        {
            case null:
                _taskCompletionSource.TrySetResult();
                break;
            case DataFusionException { ErrorCode: DataFusionErrorCode.Canceled }:
                _taskCompletionSource.TrySetCanceled();
                break;
            default:
                _taskCompletionSource.TrySetException(exception);
                break;
        }
    }
    
    internal static AsyncVoidOperation? FromHandle(IntPtr handle)
    {
        try
        {
            var h = GCHandle.FromIntPtr(handle);
            if (!h.IsAllocated)
                return null;

            return h.Target as AsyncVoidOperation;
        }
        catch (InvalidOperationException)
        {
            // The handle was freed between the IsAllocated check and the Target access.
            // Or the handle is invalid. In either case, we can just return null.
            return null;
        }
    }
}

internal class AsyncOperation<TResult> : AsyncOperation
{
    private readonly TaskCompletionSource<TResult> _taskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
    
    internal Task<TResult> Task => _taskCompletionSource.Task;
        
    internal AsyncOperation(CancellationToken cancellationToken)
        : base(cancellationToken)
    {
    }
    
    internal void Complete(TResult result)
    {
        Cleanup();
        
        _taskCompletionSource.TrySetResult(result);
    }
    
    internal void Complete(Exception exception)
    {
        Cleanup();
        
        if (exception is DataFusionException { ErrorCode: DataFusionErrorCode.Canceled })
            _taskCompletionSource.TrySetCanceled();
        else
            _taskCompletionSource.TrySetException(exception);
    }
    
    internal static AsyncOperation<TResult>? FromHandle(IntPtr handle)
    {
        try
        {
            var h = GCHandle.FromIntPtr(handle);
            if (!h.IsAllocated)
                return null;

            return h.Target as AsyncOperation<TResult>;
        }
        catch (InvalidOperationException)
        {
            // The handle was freed between the IsAllocated check and the Target access.
            // Or the handle is invalid. In either case, we can just return null.
            return null;
        }
    }
}

internal class AsyncOperation<TResult, TUserData> : AsyncOperation<TResult>
{
    internal TUserData UserData { get; }
    
    internal AsyncOperation(TUserData userData, CancellationToken cancellationToken)
        : base(cancellationToken)
    {
        UserData = userData;
    }
    
    internal new static AsyncOperation<TResult, TUserData>? FromHandle(IntPtr handle)
    {
        return AsyncOperation<TResult>.FromHandle(handle) as AsyncOperation<TResult, TUserData>;
    }
}
