using Borg;

using Hash;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateDefaultBuilder(args);
var host = builder.Build();

ulong bytesPerSecond = await StressTest.RunAsync(TimeSpan.FromSeconds(180), host.Services.GetRequiredService<ILogger<StressTest>>());
Console.WriteLine($"Speed: {HumanReadable.Bytes(bytesPerSecond)}/s");