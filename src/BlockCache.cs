namespace Hash;

using System.Diagnostics;

using Borg.Diagnostics;

using Caching;

public sealed class BlockCache: IBlockCache, IAsyncDisposable {
    readonly BlockStorage storage;
    readonly Sieve<ContentHash> evictionStrategy;
    readonly ILogger log;
    readonly int blockSize = 16 * 1024;

    enum AccessPriority {
        RELEASER,
        ACQUIRER,
    }

    readonly PrioritySemaphore<AccessPriority> accessLock = new();
    readonly Stopwatch hitRateReportStopwatch = Stopwatch.StartNew();
    long misses, hits;

    public long MaxBlockSize => this.blockSize;

    public event Action<IBlockCache, ContentHash>? Evicted;
    public event IBlockCache.AvailableHandler? Available;

    public async ValueTask<TimeSpan> WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content,
                                                CancellationToken cancel = default) {
        if (content.Length > this.blockSize)
            throw new ArgumentOutOfRangeException(nameof(content), content.Length,
                                                  "Content is too large");

        await this.accessLock.WaitAsync(AccessPriority.RELEASER, cancel).ConfigureAwait(false);
        try {
            int index;
            var start = StopwatchTimestamp.Now;
            if (this.evictionStrategy.Access(hash, out var evicted)) {
                this.Evicted?.Invoke(this, evicted);
                index = this.storage.BlockIndex(evicted);
                if (index < 0)
                    throw new InvalidProgramException(
                        "Internal error: eviction strategy record does not match storage contents");
                await this.storage.WriteAsync(index, content, hash, CancellationToken.None)
                          .ConfigureAwait(false);
                this.log.LogTrace("Set content[{Length}] for {Hash} at {Index}",
                                  content.Length, hash, index);
            } else {
#if DEBUG
                index = this.storage.BlockIndex(hash);
                this.log.LogTrace("Content[{Length}] for {Hash} is already at {Index}",
                                  content.Length, hash, index);
                Debug.Assert(index >= 0);
                return start.Elapsed;
#else
                this.log.LogTrace("Content[{Length}] for {Hash} is already in cache",
                                  content.Length, hash);
                return TimeSpan.Zero;
#endif
            }

            this.Available?.Invoke(this, hash, content.Span);
            return start.Elapsed;
        } finally {
            this.accessLock.Release();
        }
    }

    public async ValueTask<int> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                                          CancellationToken cancel = default) {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(offset, int.MaxValue);
        int offset32 = (int)offset;

        await this.accessLock.WaitAsync(AccessPriority.ACQUIRER, CancellationToken.None)
                  .ConfigureAwait(false);
        try {
            int blockIndex = this.storage.BlockIndex(hash);
            if (blockIndex < 0) {
                this.misses++;
                this.MaybeReportHitRate();
                return 0;
            }

            int read = this.storage.Read(blockIndex: blockIndex, offset: offset32, buffer, cancel);
            this.hits++;
            this.MaybeReportHitRate();
            return read;
        } finally {
            this.accessLock.Release();
        }
    }

    public async ValueTask<TimeSpan> FlushAsync(CancellationToken cancel = default) {
        await this.accessLock.WaitAsync(AccessPriority.ACQUIRER, cancel).ConfigureAwait(false);
        try {
            var start = StopwatchTimestamp.Now;
            await this.storage.FlushAsync(cancel).ConfigureAwait(false);
            return start.Elapsed;
        } finally {
            this.accessLock.Release();
        }
    }

    void MaybeReportHitRate() {
        long total = this.hits + this.misses;
        if (total == 0 || this.hitRateReportStopwatch.Elapsed <= TimeSpan.FromSeconds(60))
            return;

        this.hitRateReportStopwatch.Restart();
        this.log.LogDebug("Cache hit rate: {Rate}% ({Hits}/{Total})",
                          (double)this.hits * 100 / total,
                          this.hits, total);
    }

    BlockCache(BlockStorage storage, ILogger log) {
        this.storage = storage;
        this.evictionStrategy = new Sieve<ContentHash>(this.storage.Size);
        for (int blockIndex = 0; blockIndex < storage.Size; blockIndex++) {
            bool evicted = this.evictionStrategy.Access(storage.GetHash(blockIndex), out var __);
            Debug.Assert(!evicted);
        }

        this.log = log;
    }

    public static async Task<BlockCache> CreateAsync(string indexPath, string blocksPath,
                                                     int blockSize,
                                                     ILogger<BlockCache> log,
                                                     CancellationToken cancel = default) {
        var storage = await BlockStorage.CreateAsync(
            indexPath: indexPath,
            blocksPath: blocksPath,
            blockSize: blockSize,
            log, cancel).ConfigureAwait(false);

        return new(storage, log);
    }

    public ValueTask DisposeAsync() => this.storage.DisposeAsync();
}