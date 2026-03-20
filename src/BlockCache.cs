namespace Hash;

using System.Diagnostics;

using Borg.Diagnostics;

using Caching;

using Microsoft.VisualStudio.Threading;

public sealed class BlockCache: IBlockCache, System.IAsyncDisposable {
    public const int DEFAULT_BLOCK_SIZE = 16 * 1024;
    public static readonly int PERF_TAG = 0xACED;
    public static readonly int PERF_TAG_MASK = 0xFFFF;

    readonly BlockStorage storage;
    readonly Sieve<ContentHash> evictionStrategy;
    readonly ILogger log;
    readonly int blockSize = DEFAULT_BLOCK_SIZE;
    readonly AsyncReaderWriterLock[] blockLocks;

    enum AccessPriority {
        RELEASER,
        ACQUIRER,
    }

    readonly SemaphoreSlim indexLock = new(1);
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

        bool reportPerf = (hash.Lo4() & PERF_TAG_MASK) == PERF_TAG;
        var perfLevel = reportPerf ? LogLevel.Debug : LogLevel.Trace;
        var start = StopwatchTimestamp.Now;
        await this.indexLock.WaitAsync(cancel).ConfigureAwait(false);
        if (reportPerf)
            this.log.LogDebug("write lock wait: {Microseconds:F0}us",
                              start.Elapsed.TotalMicroseconds);

        // Hoisted outside the try so they survive the finally (indexLock.Release):
        // index is used for logging and blockWriteLock in the await-using block.
        int index = -1;
        AsyncReaderWriterLock.Releaser blockWriteLock = default;
        try {
            var writeTime = start = StopwatchTimestamp.Now;
            if (this.evictionStrategy.Access(hash, out var evicted)) {
                this.Evicted?.Invoke(this, evicted);
                index = this.storage.BlockIndex(evicted);
                if (index < 0)
                    throw new InvalidProgramException(
                        "Internal error: eviction strategy record does not match storage contents");
                if (reportPerf)
                    this.log.LogDebug("eviction: {Microseconds:F0}us",
                                      start.Elapsed.TotalMicroseconds);

                start = StopwatchTimestamp.Now;
                var entryLock = this.blockLocks[index % this.blockLocks.Length];

                // Acquire the block write lock while still holding the index lock.
                // This prevents a concurrent reader that has already found the evicted
                // hash from reading the block while we overwrite it.
                blockWriteLock = await entryLock.WriteLockAsync(cancel);
                try {
                    await this.storage.MarkDirtyAsync().ConfigureAwait(false);
                    // Phase 1 (under both locks):
                    //   (a) Write a placeholder entry to the persisted index.  This
                    //       means any crash between now and CommitFinalEntry will
                    //       leave an obviously-stale entry that crash-recovery rebuilds.
                    //   (b) Update the in-memory dict so new readers find the block
                    //       under its new hash.  Concurrent readers that arrive here
                    //       will block on the block read lock until Phase 3 completes,
                    //       so they always observe fully-written data.
                    this.storage.CommitPlaceholder(index);
                    this.storage.UpdateIndex(index, newHash: hash, oldHash: evicted);
                } catch {
                    await blockWriteLock.DisposeAsync().ConfigureAwait(false);
                    throw;
                }
            } else {
#if DEBUG
                index = this.storage.BlockIndex(hash);

                this.log.Log(perfLevel,
                             "Content[{Length}] for {Hash} is already at {Index} in {Microseconds:F0}us",
                             content.Length, hash, index, start.Elapsed.TotalMicroseconds);
                Debug.Assert(index >= 0);
                return writeTime.Elapsed;
#else
                this.log.Log(perfLevel,
                             "Content[{Length}] for {Hash} is already in cache in {Microseconds:F0}us",
                             content.Length, hash, start.Elapsed.TotalMicroseconds);
                return TimeSpan.Zero;
#endif
            }
        } finally {
            // Release the index lock before writing block data so other reads and writes
            // can proceed concurrently while the (potentially large) copy is in progress.
            this.indexLock.Release();
        }

        // index is always valid here: the early-return branches never reach this point.
        Debug.Assert(index >= 0);

        // Phase 2 (block write lock only): write the raw block bytes.
        // Phase 3 (block write lock only): commit the real persisted index entry.
        // blockWriteLock is released by the await-using; any reader that found the new
        // hash in Phase 1 and is waiting on the block read lock will unblock only after
        // both phases complete, guaranteeing it sees fully-written data.
        await using (blockWriteLock) {
            this.storage.WriteBlockData(index, content);
            this.storage.CommitFinalEntry(index, content, hash);
        }

        this.log.Log(
            perfLevel, "Set content[{Length}] for {Hash} at {Index} in {Microseconds:F0}us",
            content.Length, hash, index, start.Elapsed.TotalMicroseconds);

        this.Available?.Invoke(this, hash, content.Span);
        return start.Elapsed;
    }

    public async ValueTask<int?> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                                           CancellationToken cancel = default) {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(offset, int.MaxValue);
        int offset32 = (int)offset;

        bool reportPerf = (hash.Lo4() & PERF_TAG_MASK) == PERF_TAG;
        var start = StopwatchTimestamp.Now;
        await this.indexLock.WaitAsync(cancel).ConfigureAwait(false);
        bool haveLock = true;
        if (reportPerf)
            this.log.LogDebug("read lock wait: {Microseconds:F0}us",
                              start.Elapsed.TotalMicroseconds);
        try {
            start = StopwatchTimestamp.Now;
            int blockIndex = this.storage.BlockIndex(hash);
            if (blockIndex < 0) {
                this.misses++;
                this.MaybeReportHitRate();
                if (reportPerf)
                    this.log.LogDebug("miss: {Microseconds:F0}us", start.Elapsed.TotalMicroseconds);
                return null;
            }

            var entryLock = this.blockLocks[blockIndex % this.blockLocks.Length];
            int read;
            var readStart = StopwatchTimestamp.Now;
            await using (await entryLock.ReadLockAsync(cancel)) {
                this.indexLock.Release();
                haveLock = false;
                read = this.storage.Read(blockIndex: blockIndex, offset: offset32, buffer, cancel);
            }

            if (reportPerf)
                this.log.LogDebug("reading {Bytes} total: {TotalUS:F0}us search: {ReadUS:F0}us",
                                  read, start.Elapsed.TotalMicroseconds,
                                  readStart.Elapsed.TotalMicroseconds);
            Interlocked.Increment(ref this.hits);
            this.MaybeReportHitRate();
            return read;
        } finally {
            if (haveLock)
                this.indexLock.Release();
        }
    }

    public async ValueTask<TimeSpan> FlushAsync(CancellationToken cancel = default) {
        await this.indexLock.WaitAsync(cancel).ConfigureAwait(false);
        var lockedBlocks = new List<AsyncReaderWriterLock.Releaser>();
        try {
            foreach (var blockLock in this.blockLocks)
                lockedBlocks.Add(await blockLock.WriteLockAsync(cancel));

            var start = StopwatchTimestamp.Now;
            await this.storage.FlushAsync(cancel).ConfigureAwait(false);
            return start.Elapsed;
        } finally {
            foreach (var blockLock in lockedBlocks)
                await blockLock.DisposeAsync().ConfigureAwait(false);
            this.indexLock.Release();
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
        this.evictionStrategy = new Sieve<ContentHash>(this.storage.BlockCount);
        for (int blockIndex = 0; blockIndex < storage.BlockCount; blockIndex++) {
            bool evicted = this.evictionStrategy.Access(storage.GetHash(blockIndex), out var __);
            Debug.Assert(!evicted);
        }

        this.blockLocks =
            new AsyncReaderWriterLock[Math.Min((int)Math.Sqrt(storage.BlockCount), short.MaxValue)];
        for (int i = 0; i < this.blockLocks.Length; i++)
            this.blockLocks[i] = new AsyncReaderWriterLock(joinableTaskContext: null);

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

    public BlockCache(int blockSize, int blockCount, ILogger<BlockCache> log)
        : this(new(blockSize, blockCount, log), log) { }

    public ValueTask DisposeAsync() => this.storage.DisposeAsync();
}