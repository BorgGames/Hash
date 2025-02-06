namespace Hash;

using System.Buffers;
using System.Buffers.Binary;

using Microsoft.Extensions.Logging.Abstractions;

using Nito.AsyncEx;

using static ContentStreamClient;

class ContentStreamServer {
    readonly IBlockCache cache;
    readonly ContentStreamPacketFormat format;
    readonly Stream stream;
    readonly byte[] receiveBuffer;
    readonly CancellationTokenSource stop = new();
    StopReason stopReason = StopReason.STOP_REQUESTED;

    public enum StopReason {
        STOP_REQUESTED,
        STREAM_ENDED,
        ERROR,
    }

    public async Task<StopReason> RunAsync()
        => await this.ServeAsync(this.stop.Token).ConfigureAwait(false);

    public void Stop() => this.Stop(StopReason.STOP_REQUESTED);

    /// <seealso cref="ContentStreamClient.Connect"/>
    async Task<StopReason> ServeAsync(CancellationToken cancel) {
        byte[] sendBuffer = new byte[Math.Clamp(this.cache.MaxBlockSize,
                                                min: ContentHash.SIZE_IN_BYTES,
                                                max: 128 * 1024)];

        BinaryPrimitives.WriteInt64LittleEndian(sendBuffer, 0);
        BinaryPrimitives.WriteInt64LittleEndian(sendBuffer.AsSpan(8), this.cache.MaxBlockSize);
        try {
            await this.stream.WriteAsync(sendBuffer, 0, 16, cancel).ConfigureAwait(false);
            await this.stream.FlushAsync(cancel).ConfigureAwait(false);

            while (true) {
                await this.HandlePacketAsync(cancel).ConfigureAwait(false);
            }
        } catch (OperationCanceledException e) when (e.CancellationToken == cancel) {
            return this.stopReason;
        } catch (EndOfStreamException) {
            return StopReason.STREAM_ENDED;
        } catch (IOException) {
            return StopReason.ERROR;
        }
    }

    async ValueTask HandlePacketAsync(CancellationToken cancel) {
        var purpose = (Purpose)await this.ReadInt64Async(this.format.PurposeBytes, cancel)
                                         .ConfigureAwait(false);

        switch (purpose) {
        case Purpose.READ:
            await this.ReceiveReadAsync(cancel).ConfigureAwait(false);
            break;

        case Purpose.WRITE:
            await this.ReceiveWriteAsync(cancel).ConfigureAwait(false);
            break;

        default:
            await this.SendErrorAsync(Invariant($"bad purpose: {purpose}"), cancel)
                      .ConfigureAwait(false);
            return;
        }
    }

    async ValueTask SendErrorAsync(string message, CancellationToken cancel) {
        using var mem = MemoryPool<byte>.Shared.Rent(128);
        int bytes = System.Text.Encoding.ASCII.GetBytes(message, mem.Memory.Span);
        try {
            await this.SendAsync(mem.Memory[..bytes], cancel).ConfigureAwait(false);
        } finally {
            this.Stop(StopReason.ERROR);
        }

        throw new InvalidDataException(message);
    }

    /// <seealso cref="ContentStreamClient.WriteAsync"/>
    async ValueTask ReceiveWriteAsync(CancellationToken cancel) {
        uint queryID = await this.ReadQueryIDAsync(this.format.QueryBytes, cancel)
                                 .ConfigureAwait(false);
        int toWrite = checked((int)await this.ReadInt64Async(this.format.SizeBytes, cancel)
                                             .ConfigureAwait(false));
        var hash = await this.stream.ReadContentHash(this.receiveBuffer, cancel)
                             .ConfigureAwait(false);

        if (toWrite < 0 || toWrite > this.cache.MaxBlockSize)
            await this.SendErrorAsync(Invariant($"bad write size: {toWrite}"), cancel)
                      .ConfigureAwait(false);

        byte[] data = ArrayPool<byte>.Shared.Rent(
            Math.Max(
                toWrite,
                this.format.WriteResponseLength)); // will use the same buffer for response
        try {
            await this.stream.ReadExact(data, 0, toWrite, cancel).ConfigureAwait(false);
        } catch {
            ArrayPool<byte>.Shared.Return(data);
            throw;
        }

        this.RespondToWriteAsync(queryID, toWrite: toWrite, hash, data, cancel)
            .Forget(NullLogger<ContentStreamServer>.Instance);
    }

    /// <seealso cref="ContentStreamClient.HandlePacket"/>
    async Task RespondToWriteAsync(uint queryID, int toWrite, ContentHash hash, byte[] buffer,
                                   CancellationToken cancel) {
        try {
            var time = await this.cache.WriteAsync(hash, buffer.AsMemory(0, toWrite), cancel)
                                 .ConfigureAwait(false);
            int packetOffset = 0;
            WriteUInt64In(buffer, ref packetOffset, Purpose.WRITE.Byte(), this.format.PurposeBytes);
            WriteUInt64In(buffer, ref packetOffset, queryID, this.format.QueryBytes);
            WriteUInt64In(buffer, ref packetOffset, (ulong)time.Ticks, 8);
            await this.SendAsync(buffer.AsMemory(0, packetOffset), cancel).ConfigureAwait(false);
        } catch (Exception e) {
            string message = e is HashMismatchException ? "hash mismatch" : "internal error";
            await this.SendErrorAsync(message, cancel).ConfigureAwait(false);
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <seealso cref="ContentStreamClient.ReadAsync"/>
    async ValueTask ReceiveReadAsync(CancellationToken cancel) {
        uint queryID = await this.ReadQueryIDAsync(this.format.QueryBytes, cancel)
                                 .ConfigureAwait(false);
        long offset =
            await this.ReadInt64Async(this.format.SizeBytes, cancel).ConfigureAwait(false);
        int toRead = checked((int)await this.ReadInt64Async(this.format.SizeBytes, cancel)
                                            .ConfigureAwait(false));
        var hash = await this.stream.ReadContentHash(this.receiveBuffer, cancel)
                             .ConfigureAwait(false);

        if (toRead < 0 || toRead > this.cache.MaxBlockSize)
            await this.SendErrorAsync(Invariant($"bad read size: {toRead}"), cancel)
                      .ConfigureAwait(false);

        this.RespondToReadAsync(queryID, offset, toRead, hash, cancel)
            .Forget(NullLogger<ContentStreamServer>.Instance);
    }

    async Task RespondToReadAsync(uint queryID, long offset, int toRead, ContentHash hash,
                                  CancellationToken cancel) {
        byte[] response = ArrayPool<byte>.Shared.Rent(this.format.ReadResponseLength(toRead));

        int packetOffset = 0;
        WriteUInt64In(response, ref packetOffset, Purpose.READ.Byte(), this.format.PurposeBytes);
        int offsetOfActuallyRead = packetOffset;
        packetOffset += this.format.SizeBytes;
        WriteUInt64In(response, ref packetOffset, queryID, this.format.QueryBytes);
        try {
            var resultDataSlice = response.AsMemory(packetOffset, packetOffset + toRead);
            int? read = await this
                              .cache.ReadAsync(hash, offset, resultDataSlice, cancel)
                              .ConfigureAwait(false);

            packetOffset = offsetOfActuallyRead;
            WriteUInt64In(response, ref packetOffset,
                          (ulong)(read ?? NOT_IN_CACHE),
                          this.format.SizeBytes);

            var responseMemory = response.AsMemory(0, this.format.ReadResponseLength(read ?? 0));
            await this.SendAsync(responseMemory, cancel)
                      .ConfigureAwait(false);
        } catch {
            await this.SendErrorAsync("internal error", cancel).ConfigureAwait(false);
        } finally {
            ArrayPool<byte>.Shared.Return(response);
        }
    }

    readonly SemaphoreSlim sendSemaphore = new(1, 1);

    async ValueTask SendAsync(Memory<byte> packet, CancellationToken cancel) {
        using var _ = await this.sendSemaphore.LockAsync(cancel).ConfigureAwait(false);
        await this.stream.WriteAsync(packet, cancel).ConfigureAwait(false);
        await this.stream.FlushAsync(cancel).ConfigureAwait(false);
    }

    async ValueTask<long> ReadInt64Async(int bytes, CancellationToken cancel)
        => await this.stream.ReadInt64In(bytes, this.receiveBuffer, cancel)
                     .ConfigureAwait(false);

    async ValueTask<uint> ReadQueryIDAsync(int bytes, CancellationToken cancel) {
        int value = (int)await this.ReadInt64Async(bytes, cancel).ConfigureAwait(false);
        return (uint)value;
    }

    void Stop(StopReason reason) {
        this.stopReason = reason;
        this.stop.Cancel();
    }

    public ContentStreamServer(IBlockCache cache, Stream stream) {
        this.cache = cache ?? throw new ArgumentNullException(nameof(cache));
        this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
        if (this.cache.MaxBlockSize > int.MaxValue / 2)
            throw new NotSupportedException("Block size too large");
        if (this.cache.MaxBlockSize <= ContentHash.SIZE_IN_BYTES * 2)
            throw new NotSupportedException("Block size too small");

        this.format = ContentStreamPacketFormat.V0((int)cache.MaxBlockSize);
        this.receiveBuffer = new byte[Math.Clamp(
            this.format.WriteQueryLength((int)this.cache.MaxBlockSize),
            min: ContentHash.SIZE_IN_BYTES,
            max: 128 * 1024)];
    }
}