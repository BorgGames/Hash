namespace Hash;

public sealed class ArrayBlockIO: IBlockReader, IBlockWriter {
    readonly byte[] array;

    public ArrayBlockIO(int blockSize, long blockCount) {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockCount);

        this.BlockSize = blockSize;
        this.BlockCount = blockCount;
        this.array = new byte[checked(blockSize * blockCount)];
    }

    public int BlockSize { get; }
    public long BlockCount { get; }

    public void Read(Span<byte> buffer, long block, int offset) {
        this.CheckBounds(buffer, block, offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(block, this.BlockCount);
        long globalOffset = block * this.BlockSize + offset;
        this.array.AsSpan((int)globalOffset, buffer.Length).CopyTo(buffer);
    }

    public void Write(ReadOnlySpan<byte> buffer, long block, int offset) {
        this.CheckBounds(buffer, block, offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(block, this.BlockCount);
        long globalOffset = block * this.BlockSize + offset;
        buffer.CopyTo(this.array.AsSpan((int)globalOffset));
    }

    public ValueTask FlushAsync(CancellationToken cancel = default) => ValueTask.CompletedTask;
}