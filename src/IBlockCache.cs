namespace Hash;

public interface IBlockCache: IContentCache {
    event Action<IBlockCache, ContentHash>? Evicted;
    event AvailableHandler? Available;

    public new delegate void AvailableHandler(IBlockCache cache, ContentHash hash,
                                              ReadOnlySpan<byte> content);

    ValueTask<TimeSpan> FlushAsync(CancellationToken cancel = default);
}