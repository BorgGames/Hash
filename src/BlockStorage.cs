namespace Hash;

using System.Buffers;
using System.Diagnostics;

public sealed class BlockStorage: IAsyncDisposable {
    public static readonly ContentHash Clean = new(0xC0FFEE, 0xF0, 0xDEE, 0xC0FFEE);

    readonly ILogger log;
    readonly BlockIndex index;
    readonly IBlockWriter writer;
    readonly IBlockReader reader;
    bool dirty;

    BlockStorage(FileBlockIO blocksFile, BlockIndex index,
                 int blockSize, int size,
                 ILogger log) {
        this.writer = blocksFile ?? throw new ArgumentNullException(nameof(blocksFile));
        this.reader = blocksFile;
        this.index = index ?? throw new ArgumentNullException(nameof(index));
        this.BlockSize = blockSize;
        this.Size = size;
        this.log = log ?? throw new ArgumentNullException(nameof(log));
    }

    int StateTagPosition => this.Size;
    public int BlockSize { get; }
    public int Size { get; }
    public int Used => this.index.Used;

    public int IndexOf(ContentHash hash) {
        int index = this.index.IndexOf(hash);
        return index == this.StateTagPosition ? -1 : index;
    }

    public int SizeOfBlock(int block) => this.index[block].Bytes;

    public void Read(Span<byte> buffer, long block, int offset)
        => this.reader.Read(buffer, block, offset);

    public ContentHash GetHash(int index) {
        if (index < 0 || index >= this.Size)
            throw new ArgumentOutOfRangeException(nameof(index), index, "Index out of bounds");

        return this.index[index].Hash;
    }

    public async ValueTask Write(int index, ReadOnlyMemory<byte> block,
                                 ContentHash hash,
                                 CancellationToken cancel) {
        if (block.Length > this.BlockSize)
            throw new ArgumentOutOfRangeException(nameof(block), block.Length,
                                                  "Block size mismatch");

        if (index < 0 || index >= this.Size)
            throw new ArgumentOutOfRangeException(nameof(index), index, "Index out of bounds");

        await this.MarkDirty().ConfigureAwait(false);

        cancel.ThrowIfCancellationRequested();

        this.SetUnchecked(index, block, hash);
    }

    void SetUnchecked(int index, ReadOnlyMemory<byte> value, ContentHash hash) {
        this.index[index] = new(hash, value.Length);
        this.writer.Write(value.Span, index, offset: 0);
    }

    public void Read(int index, Memory<byte> block, CancellationToken cancel) {
        if (index < 0 || index >= this.Size)
            throw new ArgumentOutOfRangeException(nameof(index), index, "Index out of bounds");

        var entry = this.index[index];
        if (block.Length != entry.Bytes)
            throw new ArgumentOutOfRangeException(nameof(block), block.Length,
                                                  "Block size mismatch");
        if (entry.Bytes > this.BlockSize)
            throw new InvalidOperationException("Attempt to read corrupted entry");

        var destinationSlice = block[..entry.Bytes];
        this.reader.Read(destinationSlice.Span, index, offset: 0);
    }

    public async ValueTask Flush(CancellationToken cancel = default) {
        if (!this.dirty)
            return;

        var time = Stopwatch.StartNew();
        await this.writer.Flush(cancel).ConfigureAwait(false);
        var blocksTime = time.Elapsed;
        time.Restart();
        await this.index.Flush(cancel).ConfigureAwait(false);
        this.index[this.StateTagPosition] = new(Clean, 0);
        await this.index.Flush(cancel).ConfigureAwait(false);
        var indexTime = time.Elapsed;
        this.dirty = false;
        this.log.LogInformation("Flushed blocks in {Blocks}, index in {Index}",
                                blocksTime, indexTime);
    }

    async ValueTask MarkDirty() {
        if (this.dirty)
            return;

        this.index[this.StateTagPosition] = default;
        await this.index.Flush().ConfigureAwait(false);
        this.dirty = true;
    }

    public static async Task<BlockStorage> Create(string indexPath, string blocksPath,
                                                  int blockSize,
                                                  ILogger log,
                                                  CancellationToken cancel = default) {
        if (log is null) throw new ArgumentNullException(nameof(log));

        if (blockSize <= 256 / 8)
            throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize,
                                                  "Must be at least 32 bytes");
        if (blockSize <= 4096)
            log.LogWarning("Block size is less than 4KB");

        FileBlockIO? blocks = null;
        BlockIndex? index = null;
        try {
            blocks = new FileBlockIO(blocksPath, blockSize);

            long numberOfBlocks64 = blocks.Length / blockSize;
            if (numberOfBlocks64 > int.MaxValue - 3)
                throw new ArgumentOutOfRangeException(nameof(blocksPath), numberOfBlocks64,
                                                      "File is too large");
            int numberOfBlocks = (int)numberOfBlocks64;
            if (blocks.Length % blockSize != 0)
                log.LogWarning(
                    "Blocks file is not a multiple of block size. Some space will be wasted");

            await using (var indexStream = new FileStream(indexPath, FileMode.OpenOrCreate,
                                                          FileAccess.ReadWrite, FileShare.None)) {
                long indexSize = BlockIndex.Size(numberOfBlocks + 1);
                if (indexStream.Length != indexSize)
                    log.LogInformation("resizing index file to {IndexSize}", indexSize);
                else
                    log.LogInformation("index size is {IndexSize}", indexSize);
                indexStream.SetLength(indexSize);
            }

            log.LogDebug("loading index from {Path}", indexPath);
            index = await BlockIndex.Load(indexPath, checked(numberOfBlocks + 1), log, cancel)
                                    .ConfigureAwait(false);

            int stateTagPosition = numberOfBlocks;
            var stateTag = index[stateTagPosition].Hash;

            if (stateTag != Clean) await RebuildIndex(index: index,
                                   blocks: blocks,
                                   numberOfBlocks: numberOfBlocks,
                                   log, cancel).ConfigureAwait(false);

            return new(blocksFile: blocks, index,
                       blockSize: blockSize, size: numberOfBlocks,
                       log);
        } catch {
            blocks?.Dispose();
            if (blocks is not null)
                await blocks.DisposeAsync().ConfigureAwait(false);
            if (index is not null)
                await index.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    static async Task RebuildIndex(BlockIndex index,
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
            tasks.Add(RebuildIndexSlice(index, blocks,
                                        startingBlock, endingBlock,
                                        cancel));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        var rebuildTime = time.Elapsed;
        time.Restart();
        await index.Flush(cancel).ConfigureAwait(false);
        log.LogInformation("rebuilding index took {Rebuild}, writing: {Writing}",
                           rebuildTime, time.Elapsed);

        index[numberOfBlocks] = new(Clean, 0);
        await index.Flush(cancel).ConfigureAwait(false);
    }

    static async Task RebuildIndexSlice(BlockIndex index, IBlockReader blocks,
                                        int startingBlock, int endingBlock,
                                        CancellationToken cancel) {
        ArgumentOutOfRangeException.ThrowIfNegative(startingBlock);
        ArgumentOutOfRangeException.ThrowIfLessThan(endingBlock, startingBlock);

        var workChunker = new WorkChunker(TimeSpan.FromMilliseconds(10));
        using var bufferOwner = MemoryPool<byte>.Shared.Rent(blocks.BlockSize);
        var buffer = bufferOwner.Memory[..blocks.BlockSize];
        for (int blockIndex = startingBlock; blockIndex <= endingBlock; blockIndex++) {
            await workChunker.MaybeYield().ConfigureAwait(false);
            cancel.ThrowIfCancellationRequested();

            var existingEntry = index[blockIndex];
            var hash = ComputeHash(blocks, blockIndex, existingEntry, buffer.Span);
            lock (index) if (!index.TrySet(blockIndex, new(hash, existingEntry.Bytes)))
                    index[blockIndex] = new(ContentHash.Fake(), 0);
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