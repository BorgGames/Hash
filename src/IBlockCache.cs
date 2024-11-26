namespace Hash;

public interface IBlockCache {
    event Action<BlockCache, ContentHash>? Evicted;
    event AvailableHandler? Available;
    public delegate void AvailableHandler(BlockCache cache, ContentHash hash, ReadOnlySpan<byte> content);
    long MaxBlockSize { get; }

    long SizeOfBlock(ContentHash hash);
    ValueTask WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content,
                         CancellationToken cancel = default);
    ValueTask<int> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                             CancellationToken cancel = default);
}