namespace Hash;

using Borg;

public class IntegrationTests(ITestOutputHelper output) {
    [Fact]
    public async Task TcpStressTest() {
        ulong bytesPerSecond = await StressTest.RunAsync(TimeSpan.FromSeconds(30), NullLogger.Instance);
        output.WriteLine($"Speed: {HumanReadable.Bytes(bytesPerSecond)}/s");
        Assert.True(bytesPerSecond > 1024 * 1024);
    }
}