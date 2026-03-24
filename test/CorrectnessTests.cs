namespace Hash;

using Microsoft.Extensions.Logging.Abstractions;

/// <summary>
/// Correctness stress tests for <see cref="BlockCache"/>.
/// Verifies that concurrent reads and writes never produce data corruption,
/// exercising the locking logic directly without a TCP layer.
/// </summary>
public class CorrectnessTests {
    // Small block count so evictions happen frequently — maximises the chance of
    // exposing races between eviction, index update, and the data copy.
    const int BlockCount = 64;

    [Fact]
    public async Task ConcurrentReadWriteProducesNoDataCorruption() {
        var cache = new BlockCache(BlockCache.DEFAULT_BLOCK_SIZE, BlockCount,
                                   NullLogger<BlockCache>.Instance);
        await using var _ = cache;

        // RunCorrectnessAsync drives ProcessorCount*2 concurrent tasks, each writing
        // random blocks and reading them back, recomputing the hash to detect corruption.
        // A HashMismatchException is thrown (and propagated) if any mismatch is found.
        await StressTest.RunCorrectnessAsync(cache, TimeSpan.FromSeconds(15));
    }
}
