namespace Hash;

using System.Diagnostics;

using Caching;

#if FALSE
public sealed class BlockCache: IBlockCache, IAsyncDisposable {
    readonly BlockStorage storage;
    readonly Sieve<ContentHash> evictionStrategy;
    readonly ILogger log;
    readonly Dictionary<ContentHash, List<WaitRequest>> pendingReads = new();
    readonly int blockSize = 16 * 1024;

    internal enum AccessPriority {
        RELEASER,
        ACQUIRER,
        IDLE,
    }

    internal readonly PrioritySemaphore<AccessPriority> AccessLock = new();
    readonly Stopwatch hitRateReportStopwatch = Stopwatch.StartNew();
    internal long misses, hits;

    public long MaxBlockSize => this.blockSize;

    public event Action<BlockCache, ContentHash>? Evicted;
    public event IBlockCache.AvailableHandler? Available;

    public long SizeOfBlock(long block) => this.storage.SizeOfBlock(block);

    public void Read(Span<byte> buffer, long block, int offset)
        => this.storage.Read(buffer, block, offset);

    public int IndexOf(ContentHash hash) => this.storage.IndexOf(hash);

    public Task<int> WaitForBlock(ContentHash hash, int offset, ArraySegment<byte> buffer) {
        if (!this.pendingReads.TryGetValue(hash, out var list)) {
            list = new List<WaitRequest>(capacity: 4);
            this.pendingReads.Add(hash, list);
        }

        var tcs = new TaskCompletionSource<int>();
        list.Add(new() {
            Buffer = buffer,
            Offset = offset,
            Completion = tcs,
        });
        return tcs.Task;
    }

    public void Prefetch(int index) {
        // TODO implement via https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-prefetchvirtualmemory
#warning implement prefetch
    }

    readonly struct WaitRequest {
        public TaskCompletionSource<int> Completion { get; init; }
        public ArraySegment<byte> Buffer { get; init; }
        public int Offset { get; init; }
    }

    async ValueTask WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content) {
        if (content.Length > this.blockSize)
            throw new ArgumentOutOfRangeException(nameof(content), content.Length,
                                                  "Content is too large");

        if (content.Length != 16 * 1024) {
#warning implement piece length check
        }

        var actualHash = ContentHash.Compute(content.Span);

        if (actualHash != hash) {
            this.log.LogWarning("Content[{Length}] hash mismatch: {Expected} vs {Actual}",
                                content.Length, hash, actualHash);
            return;
        }

        await this.AccessLock.WaitAsync(AccessPriority.RELEASER, CancellationToken.None)
                  .ConfigureAwait(false);
        try {
            this.pendingReads.Remove(hash, out var waitingRequests);

            if (waitingRequests is not null)
                this.Satisfy(content.Span, waitingRequests);

            int index;
            if (this.evictionStrategy.Access(actualHash, out var evicted)) {
                this.Evicted?.Invoke(this, evicted);
                index = this.storage.IndexOf(evicted);
                Debug.Assert(index >= 0);
                await this.storage.Write(index, content, actualHash, default)
                          .ConfigureAwait(false);
                this.log.LogTrace("Set content[{Length}] for {Hash} at {Index}",
                                  content.Length, actualHash, index);
            } else {
                index = this.storage.IndexOf(actualHash);
                this.log.LogDebug("Content[{Length}] for {Hash} is already at {Index}",
                                  content.Length, actualHash, index);
                Debug.Assert(index >= 0);
            }

            this.Available?.Invoke(this, actualHash, content.Span);
        } finally {
            this.AccessLock.Release();
        }
    }

    void Satisfy(ReadOnlySpan<byte> buffer, List<WaitRequest> waitingRequests) {
        foreach (var request in waitingRequests) {
            var src = buffer.Slice(request.Offset);
            int copy = Math.Min(src.Length, request.Buffer.Count);
            src.Slice(0, copy).CopyTo(request.Buffer);
            Task.Run(() => request.Completion.TrySetResult(copy)).Forget(this.log);
        }
    }

    public async ValueTask<int> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                                          CancellationToken cancel = default) {
        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset), offset,
                                                  "Offset must be non-negative");

        if (buffer.Length != 16 * 1024) {
#warning implement piece length check
        }

        await this.AccessLock.WaitAsync(AccessPriority.ACQUIRER, CancellationToken.None)
                  .ConfigureAwait(false);
        try {
            int cacheIndex = this.storage.IndexOf(hash);
            if (cacheIndex < 0) {
                this.log.LogTrace("Block[{Index}/{Total}] (len {Length}) not found",
                                  query.PieceIndex, torrent.TorrentInfo.PieceCount(),
                                  buffer.Length);
                //var writeLock = await this.flushLock.WriteLockAsync();
                buffer.Span.Clear();
                return 0;
            }

            await this.storage.Read(cacheIndex, buffer, cancel).ConfigureAwait(false);
            return buffer.Length;
        } finally {
            this.AccessLock.Release();
        }
    }

    public async ValueTask Flush(CancellationToken cancel = default) {
        await this.AccessLock.WaitAsync(AccessPriority.ACQUIRER, cancel).ConfigureAwait(false);
        try {
            await this.storage.Flush(cancel).ConfigureAwait(false);
        } finally {
            this.AccessLock.Release();
        }
    }

    internal void MaybeReportHitRate() {
        long total = this.hits + Interlocked.Read(ref this.misses);
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

    public static async Task<BlockCache> Create(string indexPath, string blocksPath,
                                                int blockSize,
                                                ILogger<BlockCache> log,
                                                CancellationToken cancel = default) {
        var storage = await BlockStorage.Create(
            indexPath: indexPath,
            blocksPath: blocksPath,
            blockSize: blockSize,
            log, cancel).ConfigureAwait(false);

        return new(storage, log);
    }

    public ValueTask DisposeAsync() => this.storage.DisposeAsync();
}

#endif