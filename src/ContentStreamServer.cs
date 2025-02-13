namespace Hash;

using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;

using Microsoft.Extensions.Logging.Abstractions;

using static ContentStreamClient;

class ContentStreamServer {
    readonly IBlockCache cache;
    readonly ContentStreamPacketFormat format;
    readonly BufferedStream readStream, writeStream;
    readonly byte[] receiveBuffer;
    readonly CancellationTokenSource stop = new();
    StopReason stopReason = StopReason.STOP_REQUESTED;
    readonly ILogger log;

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
            await this.writeStream.WriteAsync(sendBuffer, 0, 16, cancel).ConfigureAwait(false);
            await this.writeStream.FlushAsync(cancel).ConfigureAwait(false);

            ExceptionDispatchInfo? exception = null;
            var thread = new Thread(() => {
                try {
                    while (!cancel.IsCancellationRequested) {
                        this.HandlePacket(cancel);
                    }
                } catch (Exception e) {
                    exception = ExceptionDispatchInfo.Capture(e);
                }
            }) {
                Name = "ContentStreamServer " + this.readStream.UnderlyingStream.GetType().Name,
                IsBackground = true,
            };
            thread.Start();
            await thread.JoinAsync().ConfigureAwait(false);
            exception?.Throw();
            return this.stopReason;
        } catch (OperationCanceledException e) when (e.CancellationToken == cancel) {
            return this.stopReason;
        } catch (EndOfStreamException) {
            return StopReason.STREAM_ENDED;
        } catch (SocketException e) when (e.SocketErrorCode is SocketError.ConnectionReset) {
            return StopReason.STREAM_ENDED;
        } catch (IOException e) {
            this.log.LogDebug(e, "IO error: {Message}", e.Message);
            return StopReason.ERROR;
        }
    }

    void HandlePacket(CancellationToken cancel) {
        var purpose = (Purpose)this.ReadInt64(this.format.PurposeBytes);

        switch (purpose) {
        case Purpose.READ:
            this.ReceiveRead(cancel);
            break;

        case Purpose.WRITE:
            this.ReceiveWrite(cancel);
            break;

        default:
            this.SendError(Invariant($"bad purpose: {purpose}"), cancel);
            return;
        }
    }

    [SuppressMessage("Usage", "VSTHRD100")]
    async void SendError(string message, CancellationToken cancel) {
        this.log.LogDebug("sending error: {Message}", message);
        using var mem = MemoryPool<byte>.Shared.Rent(128);
        int bytes = System.Text.Encoding.ASCII.GetBytes(message, mem.Memory.Span);
        try {
            await this.SendAsync(mem.Memory[..bytes], cancel).ConfigureAwait(false);
        } catch (Exception) { } finally {
            this.Stop(StopReason.ERROR);
        }
    }

    /// <seealso cref="ContentStreamClient.WriteAsync"/>
    void ReceiveWrite(CancellationToken cancel) {
        uint queryID = this.ReadQueryID(this.format.QueryBytes);
        int toWrite = checked((int)this.ReadInt64(this.format.SizeBytes));
        var hash = this.readStream.ReadContentHash(this.receiveBuffer);

        if (toWrite < 0 || toWrite > this.cache.MaxBlockSize)
            this.SendError(Invariant($"bad write size: {toWrite}"), cancel);

        byte[] data = ArrayPool<byte>.Shared.Rent(
            Math.Max(
                toWrite,
                this.format.WriteResponseLength)); // will use the same buffer for response
        try {
            this.readStream.ReadExact(data, 0, toWrite);
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
        await Task.Yield();

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
            this.SendError(message, cancel);
        } finally {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <seealso cref="ContentStreamClient.ReadAsync"/>
    void ReceiveRead(CancellationToken cancel) {
        uint queryID = this.ReadQueryID(this.format.QueryBytes);
        long offset = this.ReadInt64(this.format.SizeBytes);
        int toRead = checked((int)this.ReadInt64(this.format.SizeBytes));
        var hash = this.readStream.ReadContentHash(this.receiveBuffer);

        if (toRead < 0 || toRead > this.cache.MaxBlockSize)
            this.SendError(Invariant($"bad read size: {toRead}"), cancel);

        this.RespondToReadAsync(queryID, offset, toRead, hash, cancel)
            .Forget(NullLogger<ContentStreamServer>.Instance);
    }

    async Task RespondToReadAsync(uint queryID, long offset, int toRead, ContentHash hash,
                                  CancellationToken cancel) {
        await Task.Yield();

        byte[] response = ArrayPool<byte>.Shared.Rent(this.format.ReadResponseLength(toRead));

        int packetOffset = 0;
        WriteUInt64In(response, ref packetOffset, Purpose.READ.Byte(), this.format.PurposeBytes);
        int offsetOfActuallyRead = packetOffset;
        packetOffset += this.format.SizeBytes;
        WriteUInt64In(response, ref packetOffset, queryID, this.format.QueryBytes);
        try {
            var resultDataSlice = response.AsMemory(packetOffset, length: toRead);
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
            this.SendError("internal error", cancel);
        } finally {
            ArrayPool<byte>.Shared.Return(response);
        }
    }

    readonly ConcurrentQueue<SendRequest> sendQueue = new();
    readonly AutoResetEvent sendEvent = new(initialState: false);

    [SuppressMessage("Usage", "VSTHRD003", Justification = "We created the task")]
    async ValueTask SendAsync(Memory<byte> packet, CancellationToken cancel) {
        var request = new SendRequest(packet);
        this.sendQueue.Enqueue(request);
        this.sendEvent.Set();
        await request.Completion.Task.ConfigureAwait(false);
        await Task.Yield();
    }

    [SuppressMessage("Usage", "VSTHRD100", Justification = "We have our own thread")]
    void SendLoop() {
        //var sentSizes = new List<int>(1_000_000);
        while (!this.stop.IsCancellationRequested) {
            int sent = 0;
            while (this.sendQueue.TryDequeue(out var request)) {
                try {
                    this.writeStream.Write(request.Packet.Span);
                    request.Completion.SetResult();
                    sent++;
                } catch (Exception e) {
                    request.Completion.SetException(e);
                    this.Stop(StopReason.ERROR);
                }
            }

            if (sent > 0)
                try {
                    //sentSizes.Add(sent);
                    this.writeStream.Flush();
                } catch (Exception) {
                    this.Stop(StopReason.ERROR);
                }
            else if (this.sendQueue.IsEmpty)
                this.sendEvent.WaitOne(TimeSpan.FromSeconds(1));
        }
        //Debugger.Launch();
        //GC.KeepAlive(sentSizes);
    }

    class SendRequest {
        public SendRequest(Memory<byte> packet) => this.Packet = packet;
        public Memory<byte> Packet { get; }
        public TaskCompletionSource Completion { get; } = new();
    }

    long ReadInt64(int bytes) => this.readStream.ReadInt64In(bytes, this.receiveBuffer);

    uint ReadQueryID(int bytes) {
        int value = (int)this.ReadInt64(bytes);
        return (uint)value;
    }

    void Stop(StopReason reason) {
        if (this.stop.IsCancellationRequested)
            return;
        this.stopReason = reason;
        this.stop.Cancel();
    }

    public ContentStreamServer(IBlockCache cache, Stream stream, ILogger log) {
        if (stream is null)
            throw new ArgumentNullException(nameof(stream));
        this.cache = cache ?? throw new ArgumentNullException(nameof(cache));
        this.readStream = new(stream, bufferSize: 64 * 1024);
        this.writeStream = stream as BufferedStream ?? new(stream, bufferSize: 64 * 1024);
        this.log = log ?? throw new ArgumentNullException(nameof(log));

        if (this.cache.MaxBlockSize > int.MaxValue / 2)
            throw new NotSupportedException("Block size too large");
        if (this.cache.MaxBlockSize <= ContentHash.SIZE_IN_BYTES * 2)
            throw new NotSupportedException("Block size too small");

        this.format = ContentStreamPacketFormat.V0((int)cache.MaxBlockSize);
        this.receiveBuffer = new byte[Math.Clamp(
            this.format.WriteQueryLength((int)this.cache.MaxBlockSize),
            min: ContentHash.SIZE_IN_BYTES,
            max: 128 * 1024)];

        var sender = new Thread(this.SendLoop) {
            Name = "ContentStreamServer Send " + stream.GetType().Name,
            IsBackground = true,
        };
        sender.Start();
    }
}