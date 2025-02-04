namespace Hash;

using System.Buffers.Binary;

static class StreamExtensions {
    public static async ValueTask ReadExact(this Stream stream,
                                            byte[] buffer, int offset, int count,
                                            CancellationToken cancel) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));
        if (buffer is null)
            throw new ArgumentNullException(nameof(buffer));

        while (count > 0) {
#if NET6_0_OR_GREATER
            int read = await stream.ReadAsync(buffer.AsMemory(offset, count), cancel)
                                   .ConfigureAwait(false);
#else
            int read = await stream.ReadAsync(buffer, offset, count, cancel).ConfigureAwait(false);
#endif
            if (read == 0)
                throw new EndOfStreamException();
            offset += read;
            count -= read;
        }
    }

    public static async ValueTask ReadExact(this Stream stream, Memory<byte> buffer,
                                            byte[] tmp, CancellationToken cancel) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));

#if NET6_0_OR_GREATER
        await stream.ReadExactlyAsync(buffer, cancel).ConfigureAwait(false);
#else
        while (buffer.Length > 0) {
            int read = await stream.ReadAsync(tmp, 0, Math.Min(buffer.Length, tmp.Length), cancel)
                                   .ConfigureAwait(false);
            if (read == 0)
                throw new EndOfStreamException();
            tmp.AsMemory(0, read).CopyTo(buffer);
            buffer = buffer[read..];
        }
#endif
    }

    public static async ValueTask<long> ReadInt64In(this Stream stream, int bytes, byte[] tmp,
                                                    CancellationToken cancel) {
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (tmp == null || tmp.Length == 0) throw new ArgumentNullException(nameof(tmp));

        await stream.ReadExact(tmp, 0, bytes, cancel).ConfigureAwait(false);

        long value = bytes switch {
            1 => tmp[0],
            2 => BinaryPrimitives.ReadInt16LittleEndian(tmp),
            4 => BinaryPrimitives.ReadInt32LittleEndian(tmp),
            8 => BinaryPrimitives.ReadInt64LittleEndian(tmp),
            _ => throw new InvalidProgramException(),
        };

        return value;
    }

    public static async ValueTask<ContentHash> ReadContentHash(this Stream stream, byte[] tmp,
                                                               CancellationToken cancel) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        if (tmp is null)
            throw new ArgumentNullException(nameof(tmp));

        await stream.ReadExact(tmp, 0, ContentHash.SIZE_IN_BYTES, cancel).ConfigureAwait(false);
        return ContentHash.FromBytes(tmp.AsSpan(0, ContentHash.SIZE_IN_BYTES));
    }

    public static async ValueTask Drain(this Stream stream, long count, byte[] tmp,
                                        CancellationToken cancel) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));
        if (tmp is null)
            throw new ArgumentNullException(nameof(tmp));

        while (count > 0) {
            int toRead = (int)Math.Min(count, tmp.Length);
#if NET6_0_OR_GREATER
            int read = await stream.ReadAsync(tmp.AsMemory(0, toRead), cancel)
                                   .ConfigureAwait(false);
#else
            int read = await stream.ReadAsync(tmp, 0, toRead, cancel)
                                   .ConfigureAwait(false);
#endif
            if (read == 0)
                throw new EndOfStreamException();
            count -= read;
        }
    }
}