namespace Hash;

sealed class MappedMemoryBlockIO: IBlockReader, IBlockWriter, IDisposable {
    readonly MemoryBlockReader reader;
    readonly MemoryBlockWriter writer;
    readonly MappedFile file;

    public int BlockSize => this.reader.BlockSize;
    public long BlockCount => this.reader.BlockCount;

    public void Read(Span<byte> buffer, long block, int offset)
        => this.reader.Read(buffer, block, offset);

    public void Write(ReadOnlySpan<byte> buffer, long block, int offset)
        => this.writer.Write(buffer, block, offset);

    public async ValueTask Flush(CancellationToken cancel = default) {
        await this.writer.Flush(cancel).ConfigureAwait(false);
        await this.file.Flush(cancel).ConfigureAwait(false);
    }

    public void Dispose() => this.file.Dispose();

    public MappedMemoryBlockIO(MappedFile file, int blockSize) {
        ArgumentNullException.ThrowIfNull(file);
        this.reader = new(file.Address, file.Length, blockSize, file);
        this.writer = new(file.Address, file.Length, blockSize, file);
        this.file = file ?? throw new ArgumentNullException(nameof(file));
    }
}