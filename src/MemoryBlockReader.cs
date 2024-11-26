namespace Hash;

using Hash.Interfaces;

public sealed class MemoryBlockReader: IBlockReader {
    readonly nint address;
    readonly object? owner;
    readonly nint length;

    public int BlockSize { get; }
    public long BlockCount => this.length / this.BlockSize;

    public unsafe void Read(Span<byte> buffer, long block, int offset) {
        this.CheckBounds(buffer, block, offset);
        long globalOffset = checked(block * this.BlockSize + offset);
        long end = checked(globalOffset + buffer.Length);
        if (end > this.length || end < 0)
            throw new ArgumentOutOfRangeException(nameof(block), block, "Block out of bounds");

        var src = new ReadOnlySpan<byte>(checked((void*)(this.address + globalOffset)), buffer.Length);
        src.CopyTo(buffer);
    }

    public MemoryBlockReader(IntPtr address, nint length, int blockSize, object? owner = null) {
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