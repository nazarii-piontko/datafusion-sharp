using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal abstract class AsyncOperation
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
    
    private static long _liveCancellationTokens;
    internal static long LiveCancellationTokens => Interlocked.Read(ref _liveCancellationTokens);
#endif
    
    private static readonly IntPtr EmptyCancellationTokenHandle = IntPtr.Zero;
    private static readonly IntPtr FinishedCancellationTokenHandle = new(-1);

    private readonly CancellationToken _cancellationToken;
    private CancellationTokenRegistration _cancellationRegistration;
    
    private GCHandle _handle;
    
    // Handle to the native cancellation token associated with this operation.
    // It can be in one of three states:
    // - EmptyCancellationTokenHandle (IntPtr.Zero): the operation has not been started, and no native cancellation token has been created.
    // - FinishedCancellationTokenHandle (new IntPtr(-1)): the operation has been completed or cancellation requested, and the native cancellation token has been destroyed.
    // - Any other value: the operation is in progress, and the native cancellation token is active.
    // It is important to keep three states to prevent race condition in EnsureNativeCall and double free in Cleanup.
    private IntPtr _cancellationTokenHandle;
    
    protected AsyncOperation(CancellationToken cancellationToken)
    {
        _cancellationToken = cancellationToken;
    }

    ~AsyncOperation()
    {
        // Finalizer is a safety net to ensure resources are released even if Cleanup is not called.
        // In normal operation, Cleanup should be called explicitly to free resources.
        CleanupCore();
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
    
    internal void EnsureNativeCall(DataFusionErrorCode result, IntPtr cancellationTokenHandle, string errorMessage)
    {
        if (result != DataFusionErrorCode.Ok)
        {
            // Native call failed, the operation has not been started, clean up.
            Cleanup();
            _cancellationToken.ThrowIfCancellationRequested();
            throw new DataFusionException(result, errorMessage);
        }
        
        if (!TryInitializeCancellationTokenHandle(cancellationTokenHandle))
        {
            // As it seems the operation has been completed meanwhile, clean up.
#if MEMORY_TEST
            Interlocked.Decrement(ref _liveCancellationTokens);
#endif

            var tokenDestroyResult = NativeMethods.CancellationTokenDestroy(cancellationTokenHandle);
            Debug.Assert(tokenDestroyResult == DataFusionErrorCode.Ok, "Failed to destroy cancellation token in EnsureNativeCall.");
            return;
        }
        
        _cancellationRegistration = _cancellationToken.Register(OnCancelled);
        
        if (_cancellationToken.IsCancellationRequested)
            Cancel();
    }
    
    protected void Cleanup()
    {
        CleanupCore();
        
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
#pragma warning disable S3971
        GC.SuppressFinalize(this);
#pragma warning restore S3971
#pragma warning restore CA1816
    }
    
    protected void CleanupCore()
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
            catch (Exception)
            {
                // Handle was already freed, ignore
            }
        }
        
        var cancellationTokenHandle = TakeCancellationTokenHandle();
        if (IsValidCancellationTokenHandle(cancellationTokenHandle))
        {
#if MEMORY_TEST
            Interlocked.Decrement(ref _liveCancellationTokens);
#endif
            var tokenDestroyResult = NativeMethods.CancellationTokenDestroy(cancellationTokenHandle);
            Debug.Assert(tokenDestroyResult == DataFusionErrorCode.Ok, "Failed to destroy cancellation token in CleanupCore.");
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
    
    private void Cancel()
    {
        var cancellationTokenHandle = TakeCancellationTokenHandle();
        if (IsValidCancellationTokenHandle(cancellationTokenHandle))
        {
#if MEMORY_TEST
            Interlocked.Decrement(ref _liveCancellationTokens);
#endif

            var tokenCancelResult = NativeMethods.CancellationTokenCancel(cancellationTokenHandle);
            Debug.Assert(tokenCancelResult == DataFusionErrorCode.Ok, "Failed to cancel cancellation token in Cancel.");
        }
    }
    
    private void OnCancelled()
    {
        Cancel();
    }

    private bool TryInitializeCancellationTokenHandle(IntPtr handle)
    {
#if MEMORY_TEST
        Interlocked.Increment(ref _liveCancellationTokens);
#endif

        var prevCancellationTokenHandle = Interlocked.CompareExchange(
            ref _cancellationTokenHandle,
            handle,
            EmptyCancellationTokenHandle);
        return prevCancellationTokenHandle == EmptyCancellationTokenHandle;
    }
    
    private IntPtr TakeCancellationTokenHandle()
    {
        return Interlocked.Exchange(
            ref _cancellationTokenHandle,
            FinishedCancellationTokenHandle);
    }
    
    private static bool IsValidCancellationTokenHandle(IntPtr handle)
    {
        return handle != EmptyCancellationTokenHandle && handle != FinishedCancellationTokenHandle;
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
