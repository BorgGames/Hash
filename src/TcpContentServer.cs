namespace Hash;

using System.Net.Sockets;

public sealed class TcpContentServer {
    readonly TcpListener listener;
    readonly IBlockCache cache;
    readonly ILogger log;
    CancellationTokenSource stop = new();

    async Task RunAsync(CancellationToken cancel) {
        while (!cancel.IsCancellationRequested) {
            try {
                var client = await this.listener.AcceptTcpClientAsync(cancel).ConfigureAwait(false);
                this.log.LogDebug("{Client} connected", client.Client.RemoteEndPoint);
                this.ServeAsync(client, cancel).Forget(this.log);
            } catch (OperationCanceledException e) when (e.CancellationToken == cancel) {
                return;
            } catch (SocketException e) when (e.SocketErrorCode == SocketError.OperationAborted
                                           && cancel.IsCancellationRequested) {
                return;
            }
        }
    }

    async Task ServeAsync(TcpClient client, CancellationToken cancel) {
        var stream = client.GetStream();
        await using var _ = stream.ConfigureAwait(false);

        var server = new ContentStreamServer(this.cache, stream, this.log);
        await using var __ = cancel.Register(server.Stop);
        var stopReason = await server.RunAsync().ConfigureAwait(false);

        switch (stopReason) {
        case ContentStreamServer.StopReason.ERROR:
            this.log.LogDebug("{Client} disconnected due to error", client.Client.RemoteEndPoint);
            break;

        case ContentStreamServer.StopReason.STOP_REQUESTED:
            this.log.LogTrace("{Client} disconnected: server stopping",
                              client.Client.RemoteEndPoint);
            break;

        case ContentStreamServer.StopReason.STREAM_ENDED:
            this.log.LogDebug("{Client} disconnected", client.Client.RemoteEndPoint);
            break;

        default:
            this.log.LogWarning("{Client} disconnected: {Reason}",
                                client.Client.RemoteEndPoint, stopReason);
            break;
        }
    }

    public void Start() {
        this.listener.Start();
        this.stop = new();
        this.log.LogInformation("Listening on {EndPoint}", this.listener.LocalEndpoint);
        this.RunAsync(this.stop.Token).Forget(this.log);
    }

    public void Stop() {
        this.stop.Cancel();
        this.listener.Stop();
        this.log.LogInformation("Stopped listening on {EndPoint}", this.listener.LocalEndpoint);
    }

    public TcpContentServer(TcpListener listener, IBlockCache cache, ILogger log) {
        this.listener = listener ?? throw new ArgumentNullException(nameof(listener));
        this.cache = cache ?? throw new ArgumentNullException(nameof(cache));
        this.log = log ?? throw new ArgumentNullException(nameof(log));
    }
}