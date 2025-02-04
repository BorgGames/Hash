namespace Hash;

public interface IContentCache {
    public delegate void AvailableHandler(IContentCache cache, ContentHash hash,
                                          ReadOnlySpan<byte> content);

    long MaxBlockSize { get; }

    ValueTask<TimeSpan> WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content,
                                   CancellationToken cancel = default);

    ValueTask<int?> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                              CancellationToken cancel = default);
}