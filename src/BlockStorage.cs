namespace Hash;

using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

sealed class BlockStorage: IAsyncDisposable {
    public static readonly ContentHash Clean = new(0xC0FFEE, 0xF0, 0xDEE, 0xC0FFEE);

    readonly ILogger log;
    readonly BlockIndex index;
    readonly IBlockWriter writer;
    readonly IBlockReader reader;
    bool dirty;

    BlockStorage(MappedMemoryBlockIO blockIO, BlockIndex index,
                 int blockSize, int blockCount,
                 ILogger log) {
        this.writer = blockIO ?? throw new ArgumentNullException(nameof(blockIO));
        this.reader = blockIO;
        this.index = index ?? throw new ArgumentNullException(nameof(index));
        this.BlockSize = blockSize;
        this.BlockCount = blockCount;
        this.log = log ?? throw new ArgumentNullException(nameof(log));
    }

    [SuppressMessage("Usage", "VSTHRD002:Avoid problematic synchronous waits")]
    public BlockStorage(int blockSize, int blockCount, ILogger log) {
        this.log = log ?? throw new ArgumentNullException(nameof(log));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockCount);
        this.index = new(numberOfBlocks: checked(blockCount + 1));
        var blocks = new ArrayBlockIO(blockSize: blockSize, blockCount: blockCount);
        this.reader = blocks;
        this.writer = blocks;
        this.BlockCount = blockCount;
        this.BlockSize = blockSize;
        RebuildIndexAsync(this.index, blocks, numberOfBlocks: blockCount, log,
                          CancellationToken.None)
            .GetAwaiter().GetResult();
    }

    int StateTagPosition => this.BlockCount;
    public int BlockSize { get; }
    public int BlockCount { get; }
    public int Used => this.index.Used;

    public int BlockIndex(ContentHash hash) {
        int blockIndex = this.index.IndexOf(hash);
        return blockIndex == this.StateTagPosition ? -1 : blockIndex;
    }

    public int SizeOfBlock(int blockIndex) => this.index[blockIndex].Bytes;

    public ContentHash GetHash(int blockIndex) {
        if (blockIndex < 0 || blockIndex >= this.BlockCount)
            throw new ArgumentOutOfRangeException(nameof(blockIndex), blockIndex,
                                                  "Index out of bounds");

        return this.index[blockIndex].Hash;
    }

    public async ValueTask WriteAsync(int blockIndex, ReadOnlyMemory<byte> block,
                                      ContentHash hash,
                                      CancellationToken cancel) {
        if (block.Length > this.BlockSize)
            throw new ArgumentOutOfRangeException(nameof(block), block.Length,
                                                  "Block size mismatch");

        if (blockIndex < 0 || blockIndex >= this.BlockCount)
            throw new ArgumentOutOfRangeException(nameof(blockIndex), blockIndex,
                                                  "Index out of bounds");

        await this.MarkDirtyAsync().ConfigureAwait(false);

        cancel.ThrowIfCancellationRequested();

        this.SetUnchecked(blockIndex, block, hash);
    }

    void SetUnchecked(int blockIndex, ReadOnlyMemory<byte> value, ContentHash hash) {
        this.index[blockIndex] = new(hash, value.Length);
        this.writer.Write(value.Span, blockIndex, offset: 0);
    }

    public int Read(int blockIndex, int offset, Memory<byte> block, CancellationToken cancel) {
        if (blockIndex < 0 || blockIndex >= this.BlockCount)
            throw new ArgumentOutOfRangeException(nameof(blockIndex), blockIndex,
                                                  "Index out of bounds");

        var entry = this.index[blockIndex];
        if (entry.Bytes > this.BlockSize)
            throw new InvalidOperationException("Attempt to read corrupted entry");
        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, entry.Bytes);

        int read = Math.Min(entry.Bytes - offset, block.Length);
        var destinationSlice = block[..read];
        this.reader.Read(destinationSlice.Span, blockIndex, offset: offset);
        return read;
    }

    public async ValueTask FlushAsync(CancellationToken cancel = default) {
        if (!this.dirty)
            return;

        var time = Stopwatch.StartNew();
        await this.writer.FlushAsync(cancel).ConfigureAwait(false);
        var blocksTime = time.Elapsed;
        time.Restart();
        await this.index.FlushAsync(cancel).ConfigureAwait(false);
        this.index[this.StateTagPosition] = new(Clean, 0);
        await this.index.FlushAsync(cancel).ConfigureAwait(false);
        var indexTime = time.Elapsed;
        this.dirty = false;
        this.log.LogInformation("Flushed blocks in {Blocks}, index in {Index}",
                                blocksTime, indexTime);
    }

    async ValueTask MarkDirtyAsync() {
        if (this.dirty)
            return;

        this.index[this.StateTagPosition] = default;
        await this.index.FlushAsync().ConfigureAwait(false);
        this.dirty = true;
    }

    public static async Task<BlockStorage> CreateAsync(string indexPath, string blocksPath,
                                                       int blockSize,
                                                       ILogger log,
                                                       CancellationToken cancel = default) {
        ArgumentNullException.ThrowIfNull(log);

        if (blockSize <= 256 / 8)
            throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize,
                                                  "Must be at least 32 bytes");
        if (blockSize <= 4096)
            log.LogWarning("Block size is less than 4KB");

        var blocksFile =
            new FileInfo(blocksPath ?? throw new ArgumentNullException(nameof(blocksPath)));

        var blocks = new MappedMemoryBlockIO(blocksFile, blockSize);
        BlockIndex? index = null;
        try {
            long numberOfBlocks64 = blocks.BlockCount;
            if (numberOfBlocks64 > int.MaxValue - 3)
                throw new ArgumentOutOfRangeException(nameof(blocksPath), numberOfBlocks64,
                                                      "File is too large");
            int numberOfBlocks = (int)numberOfBlocks64;
            if (blocksFile.Length % blockSize != 0)
                log.LogWarning(
                    "Blocks file is not a multiple of block size. Some space will be wasted");

            await using (var indexStream = new FileStream(indexPath, FileMode.OpenOrCreate,
                                                          FileAccess.ReadWrite, FileShare.None)) {
                long indexSize = Hash.BlockIndex.Size(numberOfBlocks + 1);
                if (indexStream.Length != indexSize)
                    log.LogInformation("resizing index file from {OldSize} to {IndexSize}",
                                       indexStream.Length, indexSize);
                else
                    log.LogInformation("index size is {IndexSize}", indexSize);
                indexStream.SetLength(indexSize);
            }

            log.LogDebug("loading index from {Path}", indexPath);
            index = await Hash.BlockIndex
                              .LoadAsync(indexPath, checked(numberOfBlocks + 1), log, cancel)
                              .ConfigureAwait(false);

            int stateTagPosition = numberOfBlocks;
            var stateTag = index[stateTagPosition].Hash;

            if (stateTag != Clean) {
                log.LogInformation("state tag is {StateTag}, expected {Expected}",
                                   stateTag, Clean);
                await RebuildIndexAsync(index: index,
                                        blocks: blocks,
                                        numberOfBlocks: numberOfBlocks,
                                        log, cancel).ConfigureAwait(false);
            }

            return new(blockIO: blocks, index,
                       blockSize: blockSize, blockCount: numberOfBlocks,
                       log);
        } catch {
            blocks.Dispose();
            if (index is not null)
                await index.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    static async Task RebuildIndexAsync(BlockIndex index,
                                        IBlockReader blocks,
                                        int numberOfBlocks,
                                        ILogger log,
                                        CancellationToken cancel) {
        log.LogInformation("rebuilding index");
        var time = Stopwatch.StartNew();

        var tasks = new List<Task>();
        const int sliceSize = 32 * 1024;
        for (int startingBlock = 0; startingBlock < numberOfBlocks; startingBlock += sliceSize) {
            int endingBlock = Math.Min(startingBlock + sliceSize - 1, numberOfBlocks - 1);
            tasks.Add(RebuildIndexSliceAsync(index, blocks,
                                             startingBlock, endingBlock,
                                             cancel));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        var rebuildTime = time.Elapsed;
        time.Restart();
        await index.FlushAsync(cancel).ConfigureAwait(false);
        log.LogInformation("rebuilding index took {Rebuild}, writing: {Writing}",
                           rebuildTime, time.Elapsed);

        index[numberOfBlocks] = new(Clean, 0);
        await index.FlushAsync(cancel).ConfigureAwait(false);
    }

    static async Task RebuildIndexSliceAsync(BlockIndex index, IBlockReader blocks,
                                             int startingBlock, int endingBlock,
                                             CancellationToken cancel) {
        ArgumentOutOfRangeException.ThrowIfNegative(startingBlock);
        ArgumentOutOfRangeException.ThrowIfLessThan(endingBlock, startingBlock);

        var workChunker = new WorkChunker(TimeSpan.FromMilliseconds(10));
        using var bufferOwner = MemoryPool<byte>.Shared.Rent(blocks.BlockSize);
        var buffer = bufferOwner.Memory[..blocks.BlockSize];
        var random = new Random();
        for (int blockIndex = startingBlock; blockIndex <= endingBlock; blockIndex++) {
            await workChunker.MaybeYield().ConfigureAwait(false);
            cancel.ThrowIfCancellationRequested();

            var existingEntry = index[blockIndex];
            var hash = ComputeHash(blocks, blockIndex, existingEntry, buffer.Span);
            lock (index)
                if (!index.TrySet(blockIndex, new(hash, existingEntry.Bytes)))
                    index[blockIndex] = new(ContentHash.Fake(random), 0);
        }
    }

    static ContentHash ComputeHash(IBlockReader blocks, int blockIndex, BlockIndex.Entry indexEntry,
                                   Span<byte> buffer) {
        buffer = buffer[..indexEntry.Bytes];
        blocks.Read(buffer, blockIndex, offset: 0);
        var hash = ContentHash.Compute(buffer);
        return hash;
    }

    public async ValueTask DisposeAsync() {
        await this.writer.DisposeAsync(CancellationToken.None).ConfigureAwait(false);
        await this.reader.DisposeAsync(CancellationToken.None).ConfigureAwait(false);
        await this.index.DisposeAsync().ConfigureAwait(false);
    }
}