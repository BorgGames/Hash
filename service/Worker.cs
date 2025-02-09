namespace Hash;

using System.Net;
using System.Net.Sockets;

using Borg.IO;
using Borg.Threading;

using static Environment;

public class Worker: BackgroundService {
    readonly ILoggerFactory logs;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        var cacheDir =
            new DirectoryInfo(GetFolderPath(SpecialFolder.LocalApplicationData))
                .CreateSubdirectory("Borg")
                .CreateSubdirectory(nameof(Hash))
                .CreateSubdirectory("Cache");

        var blocksFile = cacheDir.File("cache.blocks");
        if (!blocksFile.Exists)
            await blocksFile.SetLength(64 * 1024L * 1024 * 1024, stoppingToken);
        var indexFile = cacheDir.File("cache.index");

        await using var cache = await BlockCache.CreateAsync(
            indexPath: indexFile.FullName,
            blocksPath: blocksFile.FullName,
            blockSize: 16 * 1024,
            this.logs.CreateLogger<BlockCache>(),
            stoppingToken);
        var validatingCache = new ValidatingBlockCache(cache);

        using var listener = new TcpListener(IPAddress.Loopback, ContentStreamClient.DEFAULT_PORT);
        var serverLog = this.logs.CreateLogger<TcpContentServer>();
        var server = new TcpContentServer(listener, validatingCache, serverLog);
        server.Start();

        await TaskEx.TryDelay(Timeout.InfiniteTimeSpan, stoppingToken);
    }

    public Worker(ILoggerFactory logs) {
        this.logs = logs ?? throw new ArgumentNullException(nameof(logs));
    }
}