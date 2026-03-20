namespace Hash;

using Borg;

using Microsoft.Extensions.Logging.Abstractions;

/// <summary>
/// Compares write throughput before and after the optimization that releases
/// <c>indexLock</c> before the 16 KiB data copy in <see cref="BlockCache.WriteAsync"/>.
/// </summary>
public class ThroughputTests(ITestOutputHelper output) {
    // Small in-memory cache — fills quickly so every write causes an eviction,
    // maximising write-path contention.
    const int BlockCount = 256;

    [Fact]
    public async Task WriteIsFasterWithIndexLockReleasedBeforeDataCopy() {
        var duration = TimeSpan.FromSeconds(10);
        var log = NullLogger<BlockCache>.Instance;

        // "Before": simulate old behaviour where indexLock was held for the full
        // write duration (including data copy) by wrapping with a global write gate.
        var innerBefore = new BlockCache(BlockCache.DEFAULT_BLOCK_SIZE, BlockCount, log);
        await using var _before = innerBefore;
        using var serialized = new GlobalWriteSerializingCache(innerBefore);
        ulong before = await StressTest.MeasureWriteThroughputAsync(serialized, duration);

        // "After": optimized BlockCache releases indexLock before the data copy,
        // allowing concurrent writes to different blocks.
        var afterCache = new BlockCache(BlockCache.DEFAULT_BLOCK_SIZE, BlockCount, log);
        await using var _after = afterCache;
        ulong after = await StressTest.MeasureWriteThroughputAsync(afterCache, duration);

        output.WriteLine(
            $"Before (serialized writes): {HumanReadable.Bytes(before)}/s");
        output.WriteLine(
            $"After  (parallel writes):   {HumanReadable.Bytes(after)}/s");

        Assert.True(after > before,
                    $"Optimized BlockCache ({HumanReadable.Bytes(after)}/s) should be faster "
                  + $"than globally-serialized writes ({HumanReadable.Bytes(before)}/s)");
    }
}

/// <summary>
/// Wraps an <see cref="IBlockCache"/> and holds a global <see cref="SemaphoreSlim"/>
/// for the full duration of every write, including the data-copy phase.
/// This reproduces the pre-optimization behaviour where <c>indexLock</c> was not
/// released until the entire write (dictionary update + 16 KiB memcpy) completed.
/// </summary>
sealed class GlobalWriteSerializingCache(IBlockCache inner) : IBlockCache, IDisposable {
    readonly SemaphoreSlim writeGate = new(1);

    public event Action<IBlockCache, ContentHash>? Evicted {
        add => inner.Evicted += value;
        remove => inner.Evicted -= value;
    }

    public event IBlockCache.AvailableHandler? Available {
        add => inner.Available += value;
        remove => inner.Available -= value;
    }

    public long MaxBlockSize => inner.MaxBlockSize;

    public async ValueTask<TimeSpan> WriteAsync(ContentHash hash, ReadOnlyMemory<byte> content,
                                                CancellationToken cancel = default) {
        await writeGate.WaitAsync(cancel).ConfigureAwait(false);
        try {
            return await inner.WriteAsync(hash, content, cancel).ConfigureAwait(false);
        } finally {
            writeGate.Release();
        }
    }

    public ValueTask<int?> ReadAsync(ContentHash hash, long offset, Memory<byte> buffer,
                                     CancellationToken cancel = default)
        => inner.ReadAsync(hash, offset, buffer, cancel);

    public ValueTask<TimeSpan> FlushAsync(CancellationToken cancel = default)
        => inner.FlushAsync(cancel);

    public void Dispose() => writeGate.Dispose();
}
