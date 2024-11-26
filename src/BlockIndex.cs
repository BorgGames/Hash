namespace Hash;

using System.Diagnostics;
using System.Runtime.InteropServices;

sealed class BlockIndex: IAsyncDisposable {
    readonly Dictionary<ContentHash, int> positions = new();
    readonly IBlockReader reader;
    readonly IBlockWriter writer;

    BlockIndex(IBlockReader reader, IBlockWriter writer, int numberOfBlocks) {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentOutOfRangeException.ThrowIfNegative(numberOfBlocks);

        if (reader.BlockSize != Marshal.SizeOf<Entry>())
            throw new ArgumentOutOfRangeException(nameof(reader));
        if (writer.BlockSize != Marshal.SizeOf<Entry>())
            throw new ArgumentOutOfRangeException(nameof(writer));

        this.reader = reader;
        this.writer = writer;
        this.Count = numberOfBlocks;
    }

    public int Used => this.positions.Count;
    public int Count { get; }

    public int IndexOf(ContentHash hash) => this.positions.GetValueOrDefault(hash, -1);

    public unsafe Entry this[int index] {
        get {
            if (index < 0 || index >= this.Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            Span<Entry> entries = stackalloc Entry[1];
            this.reader.Read(MemoryMarshal.Cast<Entry, byte>(entries), index, offset: 0);
            return entries[0];
        }

        set {
            if (!this.TrySet(index, value))
                throw new InvalidOperationException("Duplicate hash found");
        }
    }

    public bool TrySet(int index, Entry value) {
        if (index < 0 || index >= this.Count)
            throw new ArgumentOutOfRangeException(nameof(index));

        var previous = this[index];
        if (previous.Hash == value.Hash && previous.Bytes == value.Bytes) {
            this.SetUnchecked(index, value);
            return true;
        }

        if (!this.positions.TryAdd(value.Hash, index))
            return false;

        this.SetUnchecked(index, value);
        bool removed = this.positions.Remove(previous.Hash);
        Debug.Assert(removed);
        return true;
    }

    void SetUnchecked(int index, Entry value) {
        var span = MemoryMarshal.CreateSpan(ref value, 1);
        var bytes = MemoryMarshal.Cast<Entry, byte>(span);
        this.writer.Write(bytes, index, offset: 0);
    }

    public static async ValueTask<BlockIndex> LoadAsync(string path,
                                                        int numberOfBlocks,
                                                        ILogger log,
                                                        CancellationToken cancel = default) {
        ArgumentOutOfRangeException.ThrowIfNegative(numberOfBlocks);
        ArgumentNullException.ThrowIfNull(log);

        var file = new FileInfo(path ?? throw new ArgumentNullException(nameof(path)));
        var blockIO = new MappedMemoryBlockIO(file, Marshal.SizeOf<Entry>());
        var result = new BlockIndex(blockIO, blockIO, numberOfBlocks);
        var repeats = new Dictionary<ContentHash, int>();
        var workChunker = new WorkChunker(TimeSpan.FromMilliseconds(10));
        var random = new Random();
        for (int blockIndex = 0; blockIndex < numberOfBlocks; blockIndex++) {
            if (blockIndex % 128 == 0)
                await workChunker.MaybeYield().ConfigureAwait(false);

            cancel.ThrowIfCancellationRequested();

            var entry = result[blockIndex];

            if (!result.positions.TryAdd(entry.Hash, blockIndex)) {
                repeats[entry.Hash] = repeats.GetValueOrDefault(entry.Hash) + 1;
                entry = new Entry(ContentHash.Fake(random), 0);
                result.positions.Add(entry.Hash, blockIndex);
                result.SetUnchecked(blockIndex, entry);
            }
        }

        foreach (var (hash, count) in repeats)
            log.LogWarning("Duplicate hash {Hash} found {Count} times", hash, count);

        return result;
    }

    internal static Entry Read(UnmanagedMemoryAccessor accessor, int index) {
        accessor.Read(index * Marshal.SizeOf<Entry>(), out Entry entry);
        return entry;
    }

    public async ValueTask FlushAsync(CancellationToken cancel = default) {
        await this.writer.FlushAsync(cancel).ConfigureAwait(false);
    }

    public static long Size(long numberOfBlocks)
        => numberOfBlocks < 0
            ? throw new ArgumentOutOfRangeException(nameof(numberOfBlocks), numberOfBlocks,
                                                    "Must be non-negative")
            : Marshal.SizeOf<Entry>() * numberOfBlocks;

    public async ValueTask DisposeAsync() {
        await this.reader.DisposeAsync(CancellationToken.None).ConfigureAwait(false);
        await this.writer.DisposeAsync(CancellationToken.None).ConfigureAwait(false);
    }

    public readonly struct Entry(ContentHash hash, int bytes) {
        public ContentHash Hash { get; } = hash;
        public int Bytes { get; } = bytes;
    }
}