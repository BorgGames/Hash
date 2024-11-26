namespace Hash;

static class BlockIO {
    public static void CheckBounds(this IBlockIOBase blockIO,
                                   ReadOnlySpan<byte> buffer, long block, int offset) {
        if (block < 0)
            throw new ArgumentOutOfRangeException(nameof(block), block,
                                                  "Block must be non-negative");
        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset), offset,
                                                  "Offset must be non-negative");
        long bufferEnd = buffer.Length + (long)offset;
        if (bufferEnd > blockIO.BlockSize)
            throw new ArgumentOutOfRangeException(nameof(buffer), bufferEnd,
                                                  "Buffer too large for block");
    }
}