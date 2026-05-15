using Microsoft.Extensions.Logging;

namespace Strategy.Framework;

public sealed class StrategyHost
{
    private readonly List<Registration> _registrations = new();
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<StrategyHost> _logger;
    private bool _started;

    public StrategyHost(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<StrategyHost>();
    }

    public void Register(IConnector connector, StrategyBase strategy)
    {
        if (_started) throw new InvalidOperationException("Cannot register after start");
        strategy.Bind(connector, _loggerFactory.CreateLogger(strategy.GetType()));
        _registrations.Add(new Registration(connector, strategy));
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _started = true;
        foreach (var c in _registrations.Select(x => x.Connector).Distinct())
            await c.StartAsync(ct).ConfigureAwait(false);
        foreach (var r in _registrations)
            await r.Strategy.OnInitAsync(ct).ConfigureAwait(false);
        foreach (var r in _registrations)
            await r.Strategy.OnStartAsync(ct).ConfigureAwait(false);
        _logger.LogInformation("StrategyHost started with {N} strategies", _registrations.Count);
    }

    public async Task TickAsync(DateTime now, CancellationToken ct)
    {
        foreach (var r in _registrations)
            await r.Strategy.OnTickAsync(now, ct).ConfigureAwait(false);
    }

    public async Task StopAsync(CancellationToken ct)
    {
        foreach (var r in _registrations)
            try { await r.Strategy.OnStopAsync(ct).ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogError(ex, "OnStopAsync failed for {Name}", r.Strategy.Name); }
        foreach (var c in _registrations.Select(x => x.Connector).Distinct())
            try { await c.StopAsync(ct).ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogError(ex, "Connector stop failed for {Ex}", c.Exchange); }
    }

    private sealed record Registration(IConnector Connector, StrategyBase Strategy);
}
