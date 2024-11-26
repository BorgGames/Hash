namespace Hash;

public interface IBlockCache {
    event Action<IBlockCache, ContentHash>? Evicted;
    event AvailableHandler? Available;

    public delegate void AvailableHandler(IBlockCache cache, ContentHash hash,
                                          ReadOnlySpan<byte> content);

    long MaxBlockSize { get; }

    ValueTask<TimeSpan> WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content,
                                   CancellationToken cancel = default);

    ValueTask<int> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                             CancellationToken cancel = default);

    ValueTask<TimeSpan> FlushAsync(CancellationToken cancel = default);
}