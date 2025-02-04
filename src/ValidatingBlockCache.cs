namespace Hash;

using Borg.Diagnostics;

public class ValidatingBlockCache(IBlockCache cache): IBlockCache {
    readonly IBlockCache cache = cache ?? throw new ArgumentNullException(nameof(cache));

    public event Action<IBlockCache, ContentHash>? Evicted {
        add => this.cache.Evicted += value;
        remove => this.cache.Evicted -= value;
    }
    public event IBlockCache.AvailableHandler? Available {
        add => this.cache.Available += value;
        remove => this.cache.Available -= value;
    }
    public long MaxBlockSize => this.cache.MaxBlockSize;

    public async ValueTask<TimeSpan> WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content,
                                                CancellationToken cancel = default) {
        var hashStart = StopwatchTimestamp.Now;
        var actualHash = ContentHash.Compute(content.Span);
        if (hash != actualHash)
            throw new HashMismatchException();
        var timeToCheckHash = hashStart.Elapsed;
        var timeToWrite = await this.cache.WriteAsync(hash, content, cancel).ConfigureAwait(false);
        return timeToCheckHash + timeToWrite;
    }

    public ValueTask<int> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                                    CancellationToken cancel = default)
        => this.cache.ReadAsync(hash, offset, buffer, cancel);

    public ValueTask<TimeSpan> FlushAsync(CancellationToken cancel = default)
        => this.cache.FlushAsync(cancel);
}