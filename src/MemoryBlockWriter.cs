namespace Hash;

sealed class MemoryBlockWriter: IBlockWriter {
    readonly nint address;
    readonly object? owner;
    readonly nint length;

    public int BlockSize { get; }
    public long BlockCount => this.length / this.BlockSize;

    public unsafe void Write(ReadOnlySpan<byte> buffer, long block, int offset) {
        if (block < 0) throw new ArgumentOutOfRangeException(nameof(block), block, "Block must be non-negative");
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset must be non-negative");
        int bufferEnd = checked(buffer.Length + offset);
        if (bufferEnd > this.BlockSize)
            throw new ArgumentOutOfRangeException(nameof(buffer), bufferEnd, "Buffer too large for block");
        long globalOffset = checked(block * this.BlockSize + offset);
        long end = checked(globalOffset + buffer.Length);
        if (end > this.length || end < 0)
            throw new ArgumentOutOfRangeException(nameof(block), block, "Block out of bounds");

        var dst = new Span<byte>(checked((void*)(this.address + globalOffset)), buffer.Length);
        buffer.CopyTo(dst);
    }

    public ValueTask Flush(CancellationToken cancel = default) => ValueTask.CompletedTask;

    public MemoryBlockWriter(IntPtr address, nint length, int blockSize, object? owner = null) {
        if (address == IntPtr.Zero) throw new ArgumentNullException(nameof(address));
        if (length <= 0) throw new ArgumentOutOfRangeException(nameof(length));
        if (blockSize <= 0) throw new ArgumentOutOfRangeException(nameof(blockSize));
        if (length % blockSize != 0)
            throw new ArgumentException("Length must be a multiple of block size",
                                        paramName: nameof(length));

        this.address = address;
        this.length = length;
        this.BlockSize = blockSize;
        this.owner = owner;
    }
}