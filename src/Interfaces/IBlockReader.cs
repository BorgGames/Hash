namespace Hash.Interfaces;

public interface IBlockReader: IBlockIOBase {
    /// <summary>
    /// Reads data from the specified <paramref name="block"/>
    /// into the <paramref name="buffer"/>.
    /// </summary>
    /// <param name="buffer">Buffer to read data to</param>
    /// <param name="block">Index of the block to read from</param>
    /// <param name="offset">Offset within the block to read from</param>
    void Read(Span<byte> buffer, long block, int offset);
}