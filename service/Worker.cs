namespace Hash;

public class Worker: BackgroundService {
    readonly ILogger<Worker> log;

    public Worker(ILogger<Worker> log) {
        this.log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
        while (!stoppingToken.IsCancellationRequested) {
            if (this.log.IsEnabled(LogLevel.Information)) {
                this.log.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }

            await Task.Delay(1000, stoppingToken);
        }
    }
}