namespace Hash;

using System.Buffers.Binary;

static class StreamExtensions {
    public static void ReadExact(this Stream stream,
                                 byte[] buffer, int offset, int count) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));
        if (buffer is null)
            throw new ArgumentNullException(nameof(buffer));

        while (count > 0) {
            int read = stream.Read(buffer, offset, count);
            if (read == 0)
                throw new EndOfStreamException();
            offset += read;
            count -= read;
        }
    }

    public static void ReadExact(this Stream stream, Memory<byte> buffer, byte[] tmp) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));

#if NET6_0_OR_GREATER
        stream.ReadExactly(buffer.Span);
#else
        while (buffer.Length > 0) {
            int read = stream.Read(tmp, 0, Math.Min(buffer.Length, tmp.Length));
            if (read == 0)
                throw new EndOfStreamException();
            tmp.AsMemory(0, read).CopyTo(buffer);
            buffer = buffer[read..];
        }
#endif
    }

    public static long ReadInt64In(this Stream stream, int bytes, byte[] tmp) {
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (tmp == null || tmp.Length == 0) throw new ArgumentNullException(nameof(tmp));

        stream.ReadExact(tmp, 0, bytes);

        long value = bytes switch {
            1 => tmp[0],
            2 => BinaryPrimitives.ReadInt16LittleEndian(tmp),
            4 => BinaryPrimitives.ReadInt32LittleEndian(tmp),
            8 => BinaryPrimitives.ReadInt64LittleEndian(tmp),
            _ => throw new InvalidProgramException(),
        };

        return value;
    }

    public static ContentHash ReadContentHash(this Stream stream, byte[] tmp) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        if (tmp is null)
            throw new ArgumentNullException(nameof(tmp));

        stream.ReadExact(tmp, 0, ContentHash.SIZE_IN_BYTES);
        return ContentHash.FromBytes(tmp.AsSpan(0, ContentHash.SIZE_IN_BYTES));
    }

    public static void Drain(this Stream stream, long count, byte[] tmp) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));
        if (tmp is null)
            throw new ArgumentNullException(nameof(tmp));

        while (count > 0) {
            int toRead = (int)Math.Min(count, tmp.Length);
            int read = stream.Read(tmp, 0, toRead);
            if (read == 0)
                throw new EndOfStreamException();
            count -= read;
        }
    }
}