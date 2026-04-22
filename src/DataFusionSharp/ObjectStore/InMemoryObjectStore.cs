using System.Buffers;
using DataFusionSharp.Interop;

namespace DataFusionSharp.ObjectStore;

/// <summary>
/// Represents an in-memory object store that can be used with the DataFusion runtime.
/// </summary>
public sealed class InMemoryObjectStore : IDisposable
{
    /// <summary>
    /// Gets the DataFusion runtime associated with this in-memory object store.
    /// </summary>
    public DataFusionRuntime Runtime { get; }

    /// <summary>
    /// Gets the handle to the in-memory store.
    /// </summary>
    internal InMemoryStoreSafeHandle Handle { get; }
    
    internal InMemoryObjectStore(DataFusionRuntime runtime, InMemoryStoreSafeHandle inMemoryStoreHandle)
    {
        Runtime = runtime;
        Handle = inMemoryStoreHandle;
    }

    /// <summary>
    /// Puts data into the in-memory store at the specified path.
    /// The data is provided as a byte array.
    /// </summary>
    /// <remarks>
    /// The native code copies the data from the provided byte array, so the caller can safely modify or dispose of the byte array after this method returns.
    /// </remarks>
    /// <param name="path">Path to data</param>
    /// <param name="data">Bytes data to put</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task that completes when the put operation is finished.</returns>
    /// <exception cref="ArgumentException">Invalid path</exception>
    /// <exception cref="ArgumentNullException">Null data</exception>
    /// <exception cref="DataFusionException">Failed to put object into in-memory store</exception>
    public Task PutAsync(string path, Memory<byte> data, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (data.IsEmpty)
            throw new ArgumentException($"{nameof(data)} must not be empty.", nameof(data));
        
        using var pinnedData = PinnedBytesData.FromMemory(data);
        var bytesData = pinnedData.ToBytesData();
        
        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
            var result = NativeMethods.InMemoryStorePut(Handle, path, bytesData, true, &GenericCallbacks.CallbackForVoid, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to put object into in-memory store.", cancellationToken);
            
            return tcs.Task;
        }
    }

    /// <summary>
    /// Puts data into the in-memory store at the specified path.
    /// The data is provided as a pinned byte array.
    /// </summary>
    /// <remarks>
    /// The native code takes provided array as is without copying.
    /// IMPORTANT: the caller must ensure that the memory is not modified or disposed for whole life of <see cref="InMemoryObjectStore" /> instance.
    /// </remarks>
    /// <param name="path">Path to data</param>
    /// <param name="memoryHandle">Pinned memory handle to byte data to put</param>
    /// <param name="length">Length of the data in bytes</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task that completes when the put operation is finished.</returns>
    /// <exception cref="ArgumentException">Invalid path</exception>
    /// <exception cref="ArgumentNullException">Null data</exception>
    /// <exception cref="DataFusionException">Failed to put object into in-memory store</exception>
    public Task PutAsStaticAsync(string path, MemoryHandle memoryHandle, int length, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        
        var bytesData = BytesData.FromPinned(memoryHandle, length);

        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
            var result = NativeMethods.InMemoryStorePut(Handle, path, bytesData, false, &GenericCallbacks.CallbackForVoid, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to put object into in-memory store.", cancellationToken);
            
            return tcs.Task;
        }
    }

    /// <summary>
    /// Gets data from the in-memory store at the specified path.
    /// </summary>
    /// <param name="path">Path to data</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task that completes with the retrieved data as a byte array.</returns>
    /// <exception cref="ArgumentException">Invalid path</exception>
    /// <exception cref="DataFusionException">Failed to get object from in-memory store</exception>
    public Task<byte[]> GetAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create<byte[]>(cancellationToken);
            var result = NativeMethods.InMemoryStoreGet(Handle, path, &GenericCallbacks.CallbackForBytes, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to get object from in-memory store.", cancellationToken);
            
            return tcs.Task;
        }
    }

    /// <summary>
    /// Deletes data at the specified path from the in-memory store.
    /// </summary>
    /// <param name="path">Path to data to delete</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task that completes when the delete operation is finished.</returns>
    /// <exception cref="ArgumentException">Invalid path</exception>
    /// <exception cref="ArgumentNullException">Null data</exception>
    /// <exception cref="DataFusionException">Failed to delete object from in-memory store</exception>
    public Task DeleteAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
            var result = NativeMethods.InMemoryStoreDelete(Handle, path, &GenericCallbacks.CallbackForVoid, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to delete object from in-memory store.", cancellationToken);
            
            return tcs.Task;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Handle.Dispose();
    }
}