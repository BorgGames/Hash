namespace Hash;

using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

public sealed class ContentStreamClient: IContentCache, IDisposable {
    public const int DEFAULT_PORT = 16 * 1024;

    const uint QUERY_MAX = 0x0F_FF_FF_FF;
    internal const int NOT_IN_CACHE = -1;
    readonly Stream stream;

    event Action<IContentCache, ContentHash>? Evicted;
    event IContentCache.AvailableHandler? Available;
    public event Action<IContentCache, Exception>? Error;

    uint lastQueryID;
    readonly Dictionary<uint, Query> pending = new();
    readonly ContentStreamPacketFormat format;
    readonly SemaphoreSlim sendSemaphore = new(1, 1);

    public int MaxBlockSize { get; }
    long IContentCache.MaxBlockSize => this.MaxBlockSize;

    public async ValueTask<TimeSpan> WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content,
                                                CancellationToken cancel = default) {
        if (content.Length > this.MaxBlockSize)
            throw new ArgumentOutOfRangeException(nameof(content), "Content is too large");

        uint queryID = this.NextQueryID();
        int packetLength = this.format.WriteQueryLength(content.Length);

        byte[] buffer = ArrayPool<byte>.Shared.Rent(packetLength);

        int offset = 0;
        WriteUInt64In(buffer, ref offset, Purpose.WRITE.Byte(), this.format.PurposeBytes);
        WriteUInt64In(buffer, ref offset, queryID, this.format.QueryBytes);
        WriteUInt64In(buffer, ref offset, (ulong)content.Length, this.format.SizeBytes);

        hash.WriteTo(buffer.AsSpan(offset));
        offset += ContentHash.SIZE_IN_BYTES;

        content.Span.CopyTo(buffer.AsSpan(offset));

        var query = this.AddPendingQuery(queryID, new(buffer: null));

        return TimeSpan.FromTicks(await this.Send(buffer, packetLength,
                                                  queryID: queryID,
                                                  query, cancel).ConfigureAwait(false));
    }

    public async ValueTask<int?> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                                           CancellationToken cancel = default) {
        if (offset > this.MaxBlockSize)
            throw new ArgumentOutOfRangeException(nameof(offset), "Offset is too large");
        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset), "Offset cannot be negative");

        if (buffer.Length > this.MaxBlockSize)
            buffer = buffer[..this.MaxBlockSize];

        uint queryID = this.NextQueryID();
        int packetLength = this.format.PurposeBytes
                         + this.format.SizeBytes // offset
                         + this.format.SizeBytes // buffer length
                         + this.format.QueryBytes
                         + ContentHash.SIZE_IN_BYTES;

        byte[] packet = ArrayPool<byte>.Shared.Rent(packetLength);

        int packetOffset = 0;
        WriteUInt64In(packet, ref packetOffset, Purpose.READ.Byte(), this.format.PurposeBytes);
        WriteUInt64In(packet, ref packetOffset, queryID, this.format.QueryBytes);
        WriteUInt64In(packet, ref packetOffset, (ulong)offset, this.format.SizeBytes);
        WriteUInt64In(packet, ref packetOffset, (ulong)buffer.Length, this.format.SizeBytes);

        hash.WriteTo(packet.AsSpan(packetOffset));
        packetOffset += ContentHash.SIZE_IN_BYTES;

        Debug.Assert(packetOffset == packetLength);

        var query = this.AddPendingQuery(queryID, new(buffer));

        int read = checked((int)await this.Send(packet, packetLength,
                                                queryID: queryID,
                                                query, cancel).ConfigureAwait(false));
        return read >= 0 ? read : null;
    }

    readonly byte[] receiveBuffer;

    async ValueTask HandlePacket(CancellationToken cancel) {
        var purpose =
            (Purpose)await this.ReadInt64In(this.format.PurposeBytes, cancel).ConfigureAwait(false);
        switch (purpose) {
        case Purpose.READ:
            long read = await this.ReadInt64In(this.format.SizeBytes, cancel).ConfigureAwait(false);
            uint queryID = await this.ReadQueryID(this.format.QueryBytes, cancel)
                                     .ConfigureAwait(false);
            if (this.TryGetQuery(queryID) is { } query) {
                if (query.Buffer is not { } buffer)
                    throw new InvalidDataException("Wrong response purpose");
                if (read > buffer.Length)
                    throw new InvalidDataException("Cache returned too much data");
                if (read == -1) {
                    query.Completion.TrySetResult(read);
                    break;
                }

                buffer = buffer[..(int)read];
#if NET6_0_OR_GREATER
                await this.stream.ReadExactlyAsync(buffer, cancel).ConfigureAwait(false);
#else
                await this.stream.ReadExact(buffer, tmp: this.receiveBuffer, cancel)
                          .ConfigureAwait(false);
#endif

                query.Completion.TrySetResult(read);
            } else {
                if (read != -1)
                    await this.stream.Drain(count: read, this.receiveBuffer, cancel)
                              .ConfigureAwait(false);
            }

            break;

        case Purpose.WRITE:
            queryID = await this.ReadQueryID(this.format.QueryBytes, cancel).ConfigureAwait(false);
            long ticks = await this.ReadInt64In(8, cancel).ConfigureAwait(false);
            if (this.TryGetQuery(queryID) is { } pendingWrite) {
                if (pendingWrite.Buffer is not null)
                    throw new InvalidDataException("Wrong response purpose");

                pendingWrite.Completion.TrySetResult(ticks);
            } else {
                //
            }

            break;

        case Purpose.EVICTED:
            var hash = await this.stream.ReadContentHash(this.receiveBuffer, cancel)
                                 .ConfigureAwait(false);
            this.Evicted?.Invoke(this, hash);
            break;

        case Purpose.AVAILABLE:
            hash = await this.stream.ReadContentHash(this.receiveBuffer, cancel)
                             .ConfigureAwait(false);
            long blockSize =
                await this.ReadInt64In(this.format.SizeBytes, cancel).ConfigureAwait(false);
            if (blockSize > this.MaxBlockSize)
                throw new InvalidDataException();
            await this.stream.ReadExact(this.receiveBuffer, 0, (int)blockSize, cancel)
                      .ConfigureAwait(false);
            this.Available?.Invoke(this, hash, this.receiveBuffer.AsSpan(0, (int)blockSize));
            break;

        default:
            throw new InvalidDataException($"Invalid purpose: {purpose}");
        }
    }

    async ValueTask<long> Send(byte[] packet, int packetLength,
                               uint queryID, Query query,
                               CancellationToken cancel) {
        bool gotSemaphore = false;
        try {
            try {
                await this.sendSemaphore.WaitAsync(cancel).ConfigureAwait(false);
                gotSemaphore = true;
#if NET6_0_OR_GREATER
                await this.stream.WriteAsync(packet.AsMemory(0, packetLength), cancel)
                          .ConfigureAwait(false);
#else
                await this.stream.WriteAsync(packet, 0, packetLength, cancel).ConfigureAwait(false);
#endif
            } finally {
                ArrayPool<byte>.Shared.Return(packet);
            }

            await this.stream.FlushAsync(cancel).ConfigureAwait(false);

            this.sendSemaphore.Release();
            gotSemaphore = false;
        } catch {
            if (gotSemaphore)
                this.sendSemaphore.Release();
            this.Remove(queryID, query);
            throw;
        }


        using var _ = cancel.Register(() => query.Completion.TrySetCanceled(cancel),
                                      useSynchronizationContext: false);

        try {
            return await query.Completion.Task.ConfigureAwait(false);
        } finally {
            this.Remove(queryID, query);
        }
    }

    struct Query {
        public TaskCompletionSource<long> Completion = new();
        public Memory<byte>? Buffer;

        public Query(Memory<byte>? buffer) {
            this.Buffer = buffer;
        }
    }

    Query? TryGetQuery(uint queryID) {
        lock (this.pending) {
            return this.pending.TryGetValue(queryID, out var query)
                ? query
                : null;
        }
    }

    void Remove(uint queryID, Query query) {
        lock (this.pending) {
            if (this.pending.TryGetValue(queryID, out var stored)
             && stored.Completion == query.Completion)
                this.pending.Remove(queryID);
        }
    }

    Query AddPendingQuery(uint queryID, Query query) {
        lock (this.pending) {
            if (queryID == 0)
                this.Timeout(this.pending.Where(kv => kv.Key < QUERY_MAX / 2));
            if (queryID == QUERY_MAX / 2)
                this.Timeout(this.pending.Where(kv => kv.Key >= QUERY_MAX / 2));
            this.pending.Add(queryID, query);
        }

        return query;
    }

    void Timeout(IEnumerable<KeyValuePair<uint, Query>> queries) {
        foreach (var kv in queries.ToArray()) {
            kv.Value.Completion.TrySetException(new TimeoutException("QueryID wrapped around"));
            this.pending.Remove(kv.Key);
        }
    }

    uint NextQueryID() {
        lock (this.pending) {
            this.lastQueryID = (this.lastQueryID + 1) & QUERY_MAX;
            return this.lastQueryID;
        }
    }

    internal static void WriteUInt64In(Span<byte> buffer, ref int offset, ulong value, int bytes) {
        buffer = buffer[offset..];
        switch (bytes) {
        case 1:
            buffer[0] = (byte)value;
            break;
        case 2:
            BinaryPrimitives.WriteUInt16LittleEndian(buffer, (ushort)value);
            break;
        case 4:
            BinaryPrimitives.WriteUInt32LittleEndian(buffer, (uint)value);
            break;
        case 8:
            BinaryPrimitives.WriteUInt64LittleEndian(buffer, value);
            break;
        default:
            throw new InvalidProgramException();
        }

        offset += bytes;
    }

    async ValueTask<uint> ReadQueryID(int bytes, CancellationToken cancel) {
        int value = (int)await this.ReadInt64In(bytes, cancel).ConfigureAwait(false);
        return (uint)value;
    }

    ValueTask<long> ReadInt64In(int bytes, CancellationToken cancel)
        => this.stream.ReadInt64In(bytes, this.receiveBuffer, cancel);

    public static async ValueTask<ContentStreamClient> Connect(Stream stream,
                                                               CancellationToken cancel = default) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));

        byte[] buffer = new byte[sizeof(long)];
        await stream.ReadExact(buffer, 0, buffer.Length, cancel).ConfigureAwait(false);
        long version = BinaryPrimitives.ReadInt64LittleEndian(buffer);
        if (version != 0)
            throw new NotSupportedException($"Unsupported version: 0x{version:X16}");

        await stream.ReadExact(buffer, 0, buffer.Length, cancel).ConfigureAwait(false);
        long maxBlockSize = BinaryPrimitives.ReadInt64LittleEndian(buffer);
        if (maxBlockSize < ContentHash.SIZE_IN_BYTES * 2)
            throw new InvalidDataException("Invalid max block size");
        if (maxBlockSize > int.MaxValue)
            throw new NotSupportedException($"Max block size is too large: {maxBlockSize}");

        return new(stream, (int)maxBlockSize);
    }

    async void RunAsync() {
        int packets = 0;
        try {
            while (true) {
                await this.HandlePacket(CancellationToken.None).ConfigureAwait(false);
                packets++;
            }
        } catch (ObjectDisposedException) {
            return;
        } catch (Exception e) {
            this.Error?.Invoke(this, e);
        }
    }

    ContentStreamClient(Stream stream, int maxBlockSize) {
        this.stream = stream;
        this.MaxBlockSize = maxBlockSize;
#if NET6_0_OR_GREATER
        this.receiveBuffer = new byte[ContentHash.SIZE_IN_BYTES];
#else
        this.receiveBuffer =
            new byte[Math.Max(ContentHash.SIZE_IN_BYTES, Math.Min(maxBlockSize, 128 * 1024))];
#endif
        this.format = ContentStreamPacketFormat.V0(maxBlockSize);
        this.RunAsync();
    }

    public void Dispose() => this.stream.Dispose();
}