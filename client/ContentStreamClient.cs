namespace Hash;

using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;

public sealed class ContentStreamClient: IContentCache, IDisposable {
    public const int DEFAULT_PORT = 16 * 1024;

    const uint QUERY_MAX = 0x0F_FF_FF_FF;
    internal const int NOT_IN_CACHE = -1;
    readonly BufferedStream readStream, writeStream;

    event Action<IContentCache, ContentHash>? Evicted;
    event IContentCache.AvailableHandler? Available;
    public event Action<IContentCache, Exception>? Error;

    uint lastQueryID;
    readonly Dictionary<uint, Query> pending = new();
    readonly ContentStreamPacketFormat format;
    readonly CancellationTokenSource stop = new();

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

        try {
            return TimeSpan.FromTicks(await this.Send(buffer, packetLength,
                                                      queryID: queryID,
                                                      query, cancel).ConfigureAwait(false));
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
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

        try {
            int read = checked((int)await this.Send(packet, packetLength,
                                                    queryID: queryID,
                                                    query, cancel).ConfigureAwait(false));
            return read >= 0 ? read : null;
        } finally {
            ArrayPool<byte>.Shared.Return(packet);
        }
    }

    readonly byte[] receiveBuffer;

    void HandlePacket() {
        var purpose = (Purpose)this.ReadInt64In(this.format.PurposeBytes);
        switch (purpose) {
        case Purpose.READ:
            long read = this.ReadInt64In(this.format.SizeBytes);
            uint queryID = this.ReadQueryID(this.format.QueryBytes);
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
                this.readStream.ReadExactly(buffer.Span);
#else
                this.readStream.ReadExact(buffer, tmp: this.receiveBuffer);
#endif

                query.Completion.TrySetResult(read);
            } else {
                if (read != -1)
                    this.readStream.Drain(count: read, this.receiveBuffer);
            }

            break;

        case Purpose.WRITE:
            queryID = this.ReadQueryID(this.format.QueryBytes);
            long ticks = this.ReadInt64In(8);
            if (this.TryGetQuery(queryID) is { } pendingWrite) {
                if (pendingWrite.Buffer is not null)
                    throw new InvalidDataException("Wrong response purpose");

                pendingWrite.Completion.TrySetResult(ticks);
            } else {
                //
            }

            break;

        case Purpose.EVICTED:
            var hash = this.readStream.ReadContentHash(this.receiveBuffer);
            this.Evicted?.Invoke(this, hash);
            break;

        case Purpose.AVAILABLE:
            hash = this.readStream.ReadContentHash(this.receiveBuffer);
            long blockSize = this.ReadInt64In(this.format.SizeBytes);
            if (blockSize > this.MaxBlockSize)
                throw new InvalidDataException();
            this.readStream.ReadExact(this.receiveBuffer, 0, (int)blockSize);
            this.Available?.Invoke(this, hash, this.receiveBuffer.AsSpan(0, (int)blockSize));
            break;

        default:
            throw new InvalidDataException($"Invalid purpose: {purpose}");
        }
    }

    readonly ConcurrentQueue<SendRequest> sendQueue = new();
    readonly AutoResetEvent sendEvent = new(initialState: false);

    async ValueTask<long> Send(byte[] packet, int packetLength,
                               uint queryID, Query query,
                               CancellationToken cancel) {
        var request = new SendRequest(packet, packetLength, query.Completion);
        this.sendQueue.Enqueue(request);
        this.sendEvent.Set();

        using var _ = cancel.Register(() => query.Completion.TrySetCanceled(cancel),
                                      useSynchronizationContext: false);
        using var __ = this.stop.Token.Register(() => query.Completion.TrySetCanceled(this.stop.Token),
                                               useSynchronizationContext: false);

        try {
            long response = await query.Completion.Task.ConfigureAwait(false);
            await Task.Yield();
            return response;
        } finally {
            this.Remove(queryID, query);
        }
    }

    void SendLoop() {
        while (!this.stop.IsCancellationRequested) {
            SendRequest? sent = null;
            while (this.sendQueue.TryDequeue(out var request)) {
                try {
                    this.writeStream.Write(request.Packet, 0, request.PacketLength);
                    sent = request;
                } catch (Exception e) {
                    request.Completion.SetException(e);
                }
            }

            if (sent is not null)
                try {
                    this.writeStream.Flush();
                } catch (Exception e) {
                    sent.Completion.TrySetException(e);
                }
            else if (this.sendQueue.IsEmpty)
                this.sendEvent.WaitOne(TimeSpan.FromSeconds(1));
        }
    }

    class SendRequest {
        public SendRequest(byte[] packet, int packetLength, TaskCompletionSource<long> completion) {
            this.Packet = packet;
            this.PacketLength = packetLength;
            this.Completion = completion;
        }
        public byte[] Packet { get; }
        public int PacketLength { get; }
        public TaskCompletionSource<long> Completion { get; }
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

    uint ReadQueryID(int bytes) {
        int value = (int)this.ReadInt64In(bytes);
        return (uint)value;
    }

    long ReadInt64In(int bytes) => this.readStream.ReadInt64In(bytes, this.receiveBuffer);

    public static async ValueTask<ContentStreamClient> Connect(Stream stream,
                                                               CancellationToken cancel = default) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));

        byte[] buffer = new byte[sizeof(long)];
        stream.ReadExact(buffer, 0, buffer.Length);
        long version = BinaryPrimitives.ReadInt64LittleEndian(buffer);
        if (version != 0)
            throw new NotSupportedException($"Unsupported version: 0x{version:X16}");

        stream.ReadExact(buffer, 0, buffer.Length);
        long maxBlockSize = BinaryPrimitives.ReadInt64LittleEndian(buffer);
        if (maxBlockSize < ContentHash.SIZE_IN_BYTES * 2)
            throw new InvalidDataException("Invalid max block size");
        if (maxBlockSize > int.MaxValue)
            throw new NotSupportedException($"Max block size is too large: {maxBlockSize}");

        return new(stream, (int)maxBlockSize);
    }

    void Run() {
        int packets = 0;
        try {
            while (true) {
                this.HandlePacket();
                packets++;
            }
        } catch (ObjectDisposedException) {
            return;
        } catch (Exception e) {
            this.Error?.Invoke(this, e);
        }
    }

    ContentStreamClient(Stream stream, int maxBlockSize) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        this.readStream = new(stream, bufferSize: 64 * 1024);
        this.writeStream = stream as BufferedStream ?? new(stream, bufferSize: 64 * 1024);
        this.MaxBlockSize = maxBlockSize;
#if NET6_0_OR_GREATER
        this.receiveBuffer = new byte[ContentHash.SIZE_IN_BYTES];
#else
        this.receiveBuffer =
            new byte[Math.Max(ContentHash.SIZE_IN_BYTES, Math.Min(maxBlockSize, 128 * 1024))];
#endif
        this.format = ContentStreamPacketFormat.V0(maxBlockSize);
        var receiver = new Thread(this.Run) {
            IsBackground = true,
            Name = "ContentStreamClient " + stream.GetType().Name,
        };
        receiver.Start();

        var sender = new Thread(this.SendLoop) {
            Name = "ContentStreamClient Send " + stream.GetType().Name,
            IsBackground = true,
        };
        sender.Start();
    }

    public void Dispose() {
        this.stop.Cancel();
        this.readStream.Dispose();
    }
}