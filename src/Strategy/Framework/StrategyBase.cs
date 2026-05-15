using Microsoft.Extensions.Logging;

namespace Strategy.Framework;

public abstract class StrategyBase
{
    public IConnector Connector { get; private set; } = null!;
    public ILogger Logger { get; private set; } = null!;
    public string Name => GetType().Name;

    internal void Bind(IConnector connector, ILogger logger)
    {
        Connector = connector;
        Logger = logger;

        connector.OrderCreated       += e => SafeInvoke(() => OnOrderCreatedAsync(e));
        connector.OrderFilled        += e => SafeInvoke(() => OnOrderFilledAsync(e));
        connector.OrderCompleted     += e => SafeInvoke(() => OnOrderCompletedAsync(e));
        connector.OrderCancelled     += e => SafeInvoke(() => OnOrderCancelledAsync(e));
        connector.OrderRejected      += e => SafeInvoke(() => OnOrderRejectedAsync(e));
        connector.BalanceUpdated     += e => SafeInvoke(() => OnBalanceUpdatedAsync(e));
        connector.StreamStatusChanged += e => SafeInvoke(() => OnStreamStatusChangedAsync(e));
    }

    // ---- Lifecycle hooks ----
    public virtual Task OnInitAsync(CancellationToken ct)  => Task.CompletedTask;
    public virtual Task OnStartAsync(CancellationToken ct) => Task.CompletedTask;
    public virtual Task OnStopAsync(CancellationToken ct)  => Task.CompletedTask;
    public virtual Task OnTickAsync(DateTime now, CancellationToken ct) => Task.CompletedTask;

    // ---- Event hooks ----
    public virtual Task OnOrderCreatedAsync(OrderCreatedEvent e)     => Task.CompletedTask;
    public virtual Task OnOrderFilledAsync(OrderFilledEvent e)       => Task.CompletedTask;
    public virtual Task OnOrderCompletedAsync(OrderCompletedEvent e) => Task.CompletedTask;
    public virtual Task OnOrderCancelledAsync(OrderCancelledEvent e) => Task.CompletedTask;
    public virtual Task OnOrderRejectedAsync(OrderRejectedEvent e)   => Task.CompletedTask;
    public virtual Task OnBalanceUpdatedAsync(BalanceUpdatedEvent e) => Task.CompletedTask;
    public virtual Task OnStreamStatusChangedAsync(StreamStatusEvent e) => Task.CompletedTask;

    public virtual string FormatStatus() => $"[{Name}] (no status override)";

    private void SafeInvoke(Func<Task> fn)
    {
        _ = Task.Run(async () =>
        {
            try { await fn().ConfigureAwait(false); }
            catch (Exception ex) { Logger.LogError(ex, "Strategy {Name} event handler threw", Name); }
        });
    }
}
