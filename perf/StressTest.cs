namespace Hash;

using System.Net;
using System.Net.Sockets;

public class StressTest {
    public static async Task<ulong> RunAsync(TimeSpan duration, ILogger log) {
        var cacheDir = new DirectoryInfo(Path.GetTempPath())
                       .CreateSubdirectory("Borg")
                       .CreateSubdirectory("Tests")
                       .CreateSubdirectory(nameof(Hash))
                       .CreateSubdirectory(nameof(StressTest));
        var blocksFile = cacheDir.File("cache.blocks");
        if (!blocksFile.Exists)
            await blocksFile.SetLength(20 * 1024 * 1024);
        var indexFile = cacheDir.File("cache.index");
        await using var cache = await BlockCache.CreateAsync(
            indexPath: indexFile.FullName,
            blocksPath: blocksFile.FullName,
            blockSize: 16 * 1024,
            NullLogger<BlockCache>.Instance,
            CancellationToken.None);
        var validatingCache = new ValidatingBlockCache(cache);
        const int port = 13022;
        using var listener = new TcpListener(IPAddress.Loopback, port);
        var server = new TcpContentServer(listener, validatingCache, log);
        server.Start();

        var timeIsUp = duration.ToCancellation();
        var tasks = new List<Task<long>>();
        var stopwatch = StopwatchTimestamp.Now;
        var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync(IPAddress.Loopback, port, timeIsUp);
        var client = await ContentStreamClient.Connect(tcpClient.GetStream(), timeIsUp);
        for (int i = 0; i < Environment.ProcessorCount * 2; i++) {
            tasks.Add(AbuseAsync(client, timeIsUp));
        }

        long[] bytes = await Task.WhenAll(tasks);

        server.Stop();

        long totalBytes = bytes.Sum();
        var elapsed = stopwatch.Elapsed;
        ulong bytesPerSecond = (ulong)(totalBytes / elapsed.TotalSeconds);
        return bytesPerSecond;
    }

    /// <summary>
    /// Runs a correctness stress test against <paramref name="cache"/> for the specified
    /// <paramref name="duration"/> using <see cref="Environment.ProcessorCount"/> * 2
    /// concurrent read/write tasks. Each task writes random blocks and reads them back,
    /// recomputing the hash to verify data integrity. Throws
    /// <see cref="HashMismatchException"/> on corruption.
    /// </summary>
    public static async Task RunCorrectnessAsync(IContentCache cache, TimeSpan duration) {
        var timeIsUp = duration.ToCancellation();
        var tasks = new List<Task>();
        for (int i = 0; i < Environment.ProcessorCount * 2; i++)
            tasks.Add(AbuseAsync(cache, timeIsUp));

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Measures write-only throughput for the given cache over the specified
    /// <paramref name="duration"/> using <see cref="Environment.ProcessorCount"/> * 2
    /// concurrent writer tasks. Useful for isolating write-path contention.
    /// </summary>
    public static async Task<ulong> MeasureWriteThroughputAsync(IContentCache cache,
                                                                TimeSpan duration) {
        var timeIsUp = duration.ToCancellation();
        var tasks = new List<Task<long>>();
        var stopwatch = StopwatchTimestamp.Now;
        for (int i = 0; i < Environment.ProcessorCount * 2; i++)
            tasks.Add(WriteLoopAsync(cache, timeIsUp));

        long[] bytes = await Task.WhenAll(tasks);
        return (ulong)(bytes.Sum() / stopwatch.Elapsed.TotalSeconds);
    }

    static async Task<long> WriteLoopAsync(IContentCache cache, CancellationToken cancel) {
        byte[] data = new byte[cache.MaxBlockSize];
        var random = new Random();
        long transmitted = 0;
        try {
            while (!cancel.IsCancellationRequested) {
                random.NextBytes(data);
                var hash = ContentHash.Compute(data);
                await cache.WriteAsync(hash, data, cancel).ConfigureAwait(false);
                transmitted += data.Length;
            }
        } catch (OperationCanceledException) when (cancel.IsCancellationRequested) { }

        return transmitted;
    }

    static async Task<long> AbuseAsync(IContentCache cache, CancellationToken cancel) {
        byte[] data = new byte[cache.MaxBlockSize];
        var random = new Random();
        var hashes = new List<ContentHash>();
        long transmitted = 0;
        var accessStart = StopwatchTimestamp.Now;
        try {
            while (!cancel.IsCancellationRequested) {
                ContentHash hash;
                Memory<byte> block;
                if (transmitted == 0 || random.Next(10) == 0) {
                    int size = random.Next(1, (int)cache.MaxBlockSize);
                    block = data.AsMemory(0, size);
                    random.NextBytes(block.Span);
                    hash = ContentHash.Compute(block.Span);

                    using var writeCancel = TimeSpan.FromSeconds(10).ToCancellation().Link(cancel);
                    accessStart = StopwatchTimestamp.Now;
                    await cache.WriteAsync(hash, block, writeCancel.Token).ConfigureAwait(false);
                    transmitted += size;

                    hashes.Add(hash);
                }

                hash = hashes[random.Next(hashes.Count)];

                using var readCancel = TimeSpan.FromSeconds(10).ToCancellation().Link(cancel);
                accessStart = StopwatchTimestamp.Now;
                if (await cache.ReadAsync(hash, offset: 0, data, readCancel.Token)
                               .ConfigureAwait(false) is not { } read)
                    continue;

                block = data.AsMemory(0, read);
                var retrievedHash = ContentHash.Compute(block.Span);
                if (retrievedHash != hash)
                    throw new HashMismatchException();
                transmitted += read;
            }
        } catch (OperationCanceledException) {
            var opTime = accessStart.Elapsed;
            if (!cancel.IsCancellationRequested) {
                await Console.Error.WriteLineAsync($"last access: {opTime.TotalMilliseconds:N0}ms");
                throw;
            }
        }

        return transmitted;
    }
}