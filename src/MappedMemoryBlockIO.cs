namespace Hash;

public sealed class MappedMemoryBlockIO: IBlockReader, IBlockWriter, IDisposable {
    readonly MemoryBlockReader reader;
    readonly MemoryBlockWriter writer;
    readonly MappedFile file;

    public int BlockSize => this.reader.BlockSize;
    public long BlockCount => this.reader.BlockCount;

    public void Read(Span<byte> buffer, long block, int offset)
        => this.reader.Read(buffer, block, offset);

    public void Write(ReadOnlySpan<byte> buffer, long block, int offset)
        => this.writer.Write(buffer, block, offset);

    public async ValueTask FlushAsync(CancellationToken cancel = default) {
        await this.writer.FlushAsync(cancel).ConfigureAwait(false);
        await this.file.FlushAsync(cancel).ConfigureAwait(false);
    }

    public void Dispose() => this.file.Dispose();

    public MappedMemoryBlockIO(MappedFile file, int blockSize) {
        this.file = file ?? throw new ArgumentNullException(nameof(file));
        this.reader = new(file.Address, file.Length, blockSize, file);
        this.writer = new(file.Address, file.Length, blockSize, file);
    }

    public MappedMemoryBlockIO(FileInfo file, int blockSize)
        : this(MappedFile.Open(file), blockSize) { }
}