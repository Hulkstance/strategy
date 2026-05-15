using CryptoExchange.Net.SharedApis;
using Strategy.Framework;
using Strategy.Options;
using Microsoft.Extensions.Logging;

namespace Strategy.Strategies;

// Live-safe demo strategy:
//
//   1. On tick: seed reference price from the connector's locally-synced order book.
//   2. Maintain at most one open order at a time.
//      - if no open order: place a BUY `PriceOffsetPercent`% below mid, alternating
//        with a SELL `PriceOffsetPercent`% above mid. Far enough off-book that the
//        order will sit unfilled.
//      - if an open order has lived for `CancelAfterSeconds`: cancel it.
//   3. Logs every lifecycle and event hook so the full place → ack → cancel cycle is visible.
public sealed class PingPongStrategy : StrategyBase
{
    private readonly SharedSymbol _symbol;
    private readonly StrategyOptions _opts;
    private readonly TimeSpan _cancelAfter;
    private decimal _referencePrice;
    private string? _currentClientOrderId;
    private DateTime _currentPlacedAt;
    private SharedOrderSide _nextSide = SharedOrderSide.Buy;

    public PingPongStrategy(StrategyOptions opts)
    {
        _opts = opts;
        _symbol = new SharedSymbol(TradingMode.Spot, opts.BaseAsset, opts.QuoteAsset);
        _cancelAfter = TimeSpan.FromSeconds(opts.CancelAfterSeconds);
    }

    public override Task OnInitAsync(CancellationToken ct)
    {
        Logger.LogInformation("[{Name}] OnInit - exchange={Exchange}", Name, Connector.Exchange);
        return Task.CompletedTask;
    }

    public override Task OnStartAsync(CancellationToken ct)
    {
        Logger.LogInformation("[{Name}] OnStart - {Base}={BaseBal} {Quote}={QuoteBal}",
            Name, _opts.BaseAsset,  Connector.GetBalance(_opts.BaseAsset),
                  _opts.QuoteAsset, Connector.GetBalance(_opts.QuoteAsset));
        return Task.CompletedTask;
    }

    public override async Task OnTickAsync(DateTime now, CancellationToken ct)
    {
        // Seed the reference price from the live order book. Returns null until the
        // book finishes its initial snapshot, so the strategy idles for the first few ticks.
        if (_referencePrice <= 0)
        {
            var bid = Connector.GetPrice(_symbol, isBuy: false);
            var ask = Connector.GetPrice(_symbol, isBuy: true);
            if (bid is null || ask is null) return;
            _referencePrice = (bid.Value + ask.Value) / 2m;
            Logger.LogInformation("[{Name}] Reference price seeded from book: {Px} (bid={Bid} ask={Ask})",
                Name, _referencePrice, bid, ask);
        }

        // Cancel a stale open order.
        if (_currentClientOrderId is not null && now - _currentPlacedAt > _cancelAfter)
        {
            var staleCid = _currentClientOrderId;
            Logger.LogInformation("[{Name}] tick: cancelling stale order cid={Cid}", Name, staleCid);
            await Connector.CancelAsync(staleCid, ct).ConfigureAwait(false);
            _currentClientOrderId = null;  // optimistic clear; OnOrderCancelled will confirm
            return;
        }

        // Already have a live order - wait.
        if (_currentClientOrderId is not null) return;

        // No live order - place a new one, far enough from market that it sits unfilled.
        var offset = _opts.PriceOffsetPercent / 100m;
        var price = _nextSide == SharedOrderSide.Buy
            ? RoundTo(_referencePrice * (1m - offset), 1m)
            : RoundTo(_referencePrice * (1m + offset), 1m);

        var qty = SharedQuantity.Base(_opts.Quantity);
        var cid = _nextSide == SharedOrderSide.Buy
            ? await Connector.BuyAsync (_symbol, SharedOrderType.Limit, qty, price, SharedTimeInForce.GoodTillCanceled, ct)
            : await Connector.SellAsync(_symbol, SharedOrderType.Limit, qty, price, SharedTimeInForce.GoodTillCanceled, ct);

        _currentClientOrderId = cid;
        _currentPlacedAt = now;
        Logger.LogInformation(
            "[{Name}] placed {Side} {Qty} @ {Px}  cid={Cid}  openOrders={Open}",
            Name, _nextSide, _opts.Quantity, price, cid, Connector.GetOpenOrders().Count);
        _nextSide = _nextSide == SharedOrderSide.Buy ? SharedOrderSide.Sell : SharedOrderSide.Buy;
    }

    public override Task OnOrderCreatedAsync(OrderCreatedEvent e)
    {
        Logger.LogInformation("[{Name}] -> OnOrderCreated   cid={Cid} {Side} @ {Px}", Name, e.ClientOrderId, e.Side, e.Price);
        return Task.CompletedTask;
    }

    public override Task OnOrderFilledAsync(OrderFilledEvent e)
    {
        // Fires per trade. Hummingbot's did_fill_order.
        Logger.LogWarning(
            "[{Name}] -> OnOrderFilled (UNEXPECTED in safe mode)  cid={Cid} trade={Tid} {Qty} @ {Px}  fee={Fee} {Asset}  role={Role}  cum={Cum}",
            Name, e.ClientOrderId, e.TradeId, e.FillQuantity, e.FillPrice,
            e.Fee, e.FeeAsset, e.Role, e.CumulativeFilledBase);
        return Task.CompletedTask;
    }

    public override Task OnOrderCompletedAsync(OrderCompletedEvent e)
    {
        // Fires once when an order transitions to fully Filled. Hummingbot's did_complete_*.
        Logger.LogWarning(
            "[{Name}] -> OnOrderCompleted (UNEXPECTED in safe mode)  cid={Cid} avg={Avg} total={Total}",
            Name, e.ClientOrderId, e.AveragePrice, e.TotalFilledBase);
        _currentClientOrderId = null;
        return Task.CompletedTask;
    }

    public override Task OnOrderCancelledAsync(OrderCancelledEvent e)
    {
        Logger.LogInformation("[{Name}] -> OnOrderCancelled cid={Cid}", Name, e.ClientOrderId);
        _currentClientOrderId = null;
        return Task.CompletedTask;
    }

    public override Task OnOrderRejectedAsync(OrderRejectedEvent e)
    {
        Logger.LogWarning("[{Name}] -> OnOrderRejected  cid={Cid} reason={Reason}", Name, e.ClientOrderId, e.Reason);
        _currentClientOrderId = null;
        return Task.CompletedTask;
    }

    public override Task OnStreamStatusChangedAsync(StreamStatusEvent e)
    {
        Logger.LogInformation("[{Name}] -> StreamStatus {Stream}: {State}",
            Name, e.Stream, e.Connected ? "Restored" : "Interrupted");
        return Task.CompletedTask;
    }

    public override Task OnBalanceUpdatedAsync(BalanceUpdatedEvent e)
    {
        // Exchange balance pushes are fairly chatty; log only assets we trade.
        if (e.Asset == _opts.BaseAsset || e.Asset == _opts.QuoteAsset)
            Logger.LogInformation("[{Name}] -> OnBalanceUpdated {Asset}: avail={Avail} total={Total}",
                Name, e.Asset, e.Available, e.Total);
        return Task.CompletedTask;
    }

    public override string FormatStatus() =>
        $"[{Name}] refPx={_referencePrice} nextSide={_nextSide} currentOrder={_currentClientOrderId ?? "(none)"}";

    private static decimal RoundTo(decimal value, decimal step)
        => Math.Round(value / step, MidpointRounding.AwayFromZero) * step;
}
