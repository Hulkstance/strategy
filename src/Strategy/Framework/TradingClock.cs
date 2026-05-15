using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Strategy.Framework;

public sealed class TradingClock : BackgroundService
{
    private readonly StrategyHost _host;
    private readonly ILogger<TradingClock> _logger;
    public TimeSpan TickInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TradingClock(StrategyHost host, ILogger<TradingClock> logger)
    {
        _host = host;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _host.StartAsync(stoppingToken).ConfigureAwait(false);
        using var timer = new PeriodicTimer(TickInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                var now = DateTime.UtcNow;
                try
                {
                    await _host.TickAsync(now, stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Tick failed at {Now}", now);
                }
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        finally
        {
            await _host.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
