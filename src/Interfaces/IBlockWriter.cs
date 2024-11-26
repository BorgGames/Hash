namespace Hash.Interfaces;

public interface IBlockWriter: IBlockIOBase {
    /// <summary>
    /// Writes data to the specified <paramref name="block"/>
    /// from the <paramref name="buffer"/>.
    /// </summary>
    /// <param name="buffer">Data to write</param>
    /// <param name="block">Index of the block to write to</param>
    /// <param name="offset">Offset within the block to write to</param>
    void Write(ReadOnlySpan<byte> buffer, long block, int offset);
    ValueTask FlushAsync(CancellationToken cancel = default);
}
