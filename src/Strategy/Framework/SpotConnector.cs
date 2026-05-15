using CryptoClients.Net.Interfaces;
using CryptoExchange.Net.Interfaces;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.SharedApis;
using CryptoExchange.Net.Trackers.UserData.Interfaces;
using CryptoExchange.Net.Trackers.UserData.Objects;
using Microsoft.Extensions.Logging;

namespace Strategy.Framework;

// SpotConnector - Hummingbot-style facade over the JKorf stack.
//
//   user state  ──>  IUserSpotDataTracker (ws + REST poll, in-memory ConcurrentDictionary)
//   writes      ──>  ISpotOrderRestClient.PlaceSpotOrderAsync (one-shot REST)
//   events      ──>  IUserDataTracker<SharedSpotOrder>.OnUpdate, demultiplexed on status transitions
//   prices      ──>  ISymbolOrderBook per tracked symbol (locally-synced book, ws + snapshot)
//
// The optimistic order cache fills the gap that Hummingbot's ClientOrderTracker covers:
// freshly placed orders are visible to the strategy before the tracker's next poll/ws
// update reflects them.
public sealed class SpotConnector : IConnector
{
    private readonly IUserSpotDataTracker _tracker;
    private readonly ISpotOrderRestClient _orderClient;
    private readonly ISpotSymbolRestClient _symbolClient;
    private readonly IExchangeOrderBookFactory _bookFactory;
    private readonly ILogger<SpotConnector> _logger;
    private readonly OptimisticOrderCache _optimistic = new();
    // Per-order state we accumulate to emit higher-level events.
    //   _lastStatus           - last SharedOrderStatus we processed; used to detect
    //                           Open->Filled / Open->Canceled transitions exactly once.
    //   _cumulativeByOrder    - running sum of base-asset quantities seen so far;
    //                           reported as CumulativeFilledBase on OrderFilled.
    //   _seenTradeIdsByOrder  - TradeIds we've already emitted OrderFilled for, so
    //                           the LastTrade race-closer in HandleOrderUpdate doesn't
    //                           double-fire if Trades.OnUpdate also delivers the same
    //                           trade. Cleared on terminal status.
    private readonly Dictionary<string, SharedOrderStatus> _lastStatus = new();
    private readonly Dictionary<string, decimal> _cumulativeByOrder = new();
    private readonly Dictionary<string, HashSet<string>> _seenTradeIdsByOrder = new();
    private readonly object _stateLock = new();
    private readonly Dictionary<string, SharedSpotSymbol> _tradingRules =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ISymbolOrderBook> _books =
        new(StringComparer.OrdinalIgnoreCase);

    public string Exchange => _tracker.Exchange;
    public bool Ready => _tracker.Connected;

    public event Action<OrderCreatedEvent>?   OrderCreated;
    public event Action<OrderFilledEvent>?    OrderFilled;
    public event Action<OrderCompletedEvent>? OrderCompleted;
    public event Action<OrderCancelledEvent>? OrderCancelled;
    public event Action<OrderRejectedEvent>?  OrderRejected;
    public event Action<BalanceUpdatedEvent>? BalanceUpdated;
    public event Action<StreamStatusEvent>?   StreamStatusChanged;

    public SpotConnector(
        IUserSpotDataTracker tracker,
        ISpotOrderRestClient orderClient,
        ISpotSymbolRestClient symbolClient,
        IExchangeOrderBookFactory bookFactory,
        ILogger<SpotConnector> logger)
    {
        _tracker = tracker;
        _orderClient = orderClient;
        _symbolClient = symbolClient;
        _bookFactory = bookFactory;
        _logger = logger;

        _tracker.Orders.OnUpdate    += HandleOrderUpdate;
        _tracker.Balances.OnUpdate  += HandleBalanceUpdate;
        _tracker.Trades!.OnUpdate   += HandleTradeUpdate;
        // Match CryptoManager.Net.Subscriptions.User.UserSubscriptionService.cs:91-94 -
        // forward all four channels including stream-status to the strategy.
        _tracker.OnConnectedChange  += (type, connected) =>
            StreamStatusChanged?.Invoke(new StreamStatusEvent(Exchange, type, connected, DateTime.UtcNow));
    }

    public decimal GetBalance(string asset) =>
        _tracker.Balances.Values.FirstOrDefault(b => b.Asset == asset)?.Total ?? 0m;

    public decimal GetAvailableBalance(string asset) =>
        _tracker.Balances.Values.FirstOrDefault(b => b.Asset == asset)?.Available ?? 0m;

    public IReadOnlyCollection<SharedSpotOrder> GetOpenOrders() =>
        _tracker.Orders.Values
            .Where(o => o.Status == SharedOrderStatus.Open)
            .Concat(_optimistic.PendingOrders)
            .ToList();

    public SharedSpotOrder? GetOrder(string clientOrderId)
    {
        var tracked = _tracker.Orders.Values.FirstOrDefault(o => o.ClientOrderId == clientOrderId);
        return tracked ?? _optimistic.GetPending(clientOrderId);
    }

    public void SetTradingRules(IEnumerable<SharedSpotSymbol> symbols)
    {
        foreach (var s in symbols) _tradingRules[s.Name] = s;
    }

    public SharedSpotSymbol? GetTradingRules(SharedSymbol symbol)
    {
        var name = symbol.GetSymbol((b, q, _, _) => b + q);
        if (_tradingRules.TryGetValue(name, out var rules)) return rules;
        return _tradingRules.Values.FirstOrDefault(s =>
            s.BaseAsset == symbol.BaseAsset && s.QuoteAsset == symbol.QuoteAsset);
    }

    public decimal? GetPrice(SharedSymbol symbol, bool isBuy)
    {
        if (!_books.TryGetValue(BookKey(symbol), out var book)) return null;
        if (book.Status != OrderBookStatus.Synced) return null;
        var entry = isBuy ? book.BestAsk : book.BestBid;
        return entry.Price == 0m ? null : entry.Price;
    }

    private static string BookKey(SharedSymbol s) => $"{s.BaseAsset}_{s.QuoteAsset}";

    public Task<string> BuyAsync(SharedSymbol s, SharedOrderType t, SharedQuantity q,
        decimal? p = null, SharedTimeInForce? tif = null, CancellationToken ct = default)
        => PlaceAsync(SharedOrderSide.Buy, s, t, q, p, tif, ct);

    public Task<string> SellAsync(SharedSymbol s, SharedOrderType t, SharedQuantity q,
        decimal? p = null, SharedTimeInForce? tif = null, CancellationToken ct = default)
        => PlaceAsync(SharedOrderSide.Sell, s, t, q, p, tif, ct);

    private async Task<string> PlaceAsync(
        SharedOrderSide side, SharedSymbol symbol, SharedOrderType orderType,
        SharedQuantity quantity, decimal? price, SharedTimeInForce? tif, CancellationToken ct)
    {
        var clientOrderId = _orderClient.GenerateClientOrderId();

        // Optimistic insert + fire OrderCreated BEFORE the REST call returns.
        var pending = new SharedSpotOrder(
            sharedSymbol: symbol,
            symbol: symbol.GetSymbol((b, q, _, _) => b + q),
            orderId: $"PENDING:{clientOrderId}",
            orderType: orderType,
            orderSide: side,
            orderStatus: SharedOrderStatus.Open,
            createTime: DateTime.UtcNow)
        {
            ClientOrderId = clientOrderId,
            OrderPrice = price,
            OrderQuantity = new SharedOrderQuantity(
                baseAssetQuantity: quantity.QuantityInBaseAsset,
                quoteAssetQuantity: quantity.QuantityInQuoteAsset)
        };
        _optimistic.AddPending(pending);
        OrderCreated?.Invoke(new OrderCreatedEvent(
            Exchange, clientOrderId, null, symbol, side, orderType, price, quantity, DateTime.UtcNow));

        var request = new PlaceSpotOrderRequest(symbol, side, orderType, quantity, price, tif, clientOrderId);
        var result = await _orderClient.PlaceSpotOrderAsync(request, ct).ConfigureAwait(false);

        if (!result.Success)
        {
            _optimistic.RemovePending(clientOrderId);
            OrderRejected?.Invoke(new OrderRejectedEvent(
                Exchange, clientOrderId, symbol, result.Error?.Message ?? "Unknown error", DateTime.UtcNow));
            _logger.LogWarning("Order {Cid} rejected by {Exchange}: {Err}", clientOrderId, Exchange, result.Error);
        }
        return clientOrderId;
    }

    public async Task CancelAsync(string clientOrderId, CancellationToken ct = default)
    {
        // Tolerant of unknown ids: callers may race a Cancel against an OrderRejected
        // that already removed the order from cache, or against the tracker dropping
        // a terminally-resolved entry. Log and no-op rather than throw - matches the
        // CallResult-style "no exceptions" contract used throughout the JKorf stack.
        var order = GetOrder(clientOrderId);
        if (order is null)
        {
            _logger.LogWarning("Cancel for {Cid} ignored - order not in cache", clientOrderId);
            return;
        }
        if (order.OrderId.StartsWith("PENDING:"))
        {
            _logger.LogWarning("Order {Cid} has no exchange ack yet - refusing to cancel", clientOrderId);
            return;
        }
        var req = new CancelOrderRequest(order.SharedSymbol!, order.OrderId);
        var result = await _orderClient.CancelSpotOrderAsync(req, ct).ConfigureAwait(false);
        if (!result.Success)
            _logger.LogWarning("Cancel for {Cid} failed: {Err}", clientOrderId, result.Error);
    }

    // Orders.OnUpdate carries *both* status transitions (OrderCompleted/OrderCancelled)
    // AND, when the exchange/path supports it, the trade that caused the latest fill
    // via SharedSpotOrder.LastTrade. Processing the LastTrade *before* the status
    // transition closes the race that exists when trade and order events arrive on
    // independent channels (websocket topics + REST poll loops).
    //
    // Trades.OnUpdate is the canonical source of OrderFilled (per CryptoManager.Net /
    // Hummingbot did_fill_order). TryEmitOrderFilled dedups by trade.Id so the same
    // fill emitted via both paths does not double-fire.
    private Task HandleOrderUpdate(UserDataUpdate<SharedSpotOrder[]> update)
    {
        _optimistic.ReconcileFromTracker(update.Data);

        foreach (var order in update.Data)
        {
            var cid = order.ClientOrderId ?? order.OrderId;

            // Race-closer: if this order update carries the trade that produced the
            // fill, emit OrderFilled now - before potentially firing OrderCompleted.
            if (order.LastTrade is { } lastTrade)
                TryEmitOrderFilled(lastTrade, order.SharedSymbol!);

            SharedOrderStatus? prevStatus;
            lock (_stateLock)
            {
                prevStatus = _lastStatus.TryGetValue(cid, out var p) ? p : null;
                _lastStatus[cid] = order.Status;
            }

            if (order.Status == SharedOrderStatus.Filled && prevStatus != SharedOrderStatus.Filled)
            {
                OrderCompleted?.Invoke(new OrderCompletedEvent(
                    Exchange, cid, order.OrderId, order.SharedSymbol!, order.Side,
                    AveragePrice: order.AveragePrice ?? 0m,
                    TotalFilledBase: order.QuantityFilled?.QuantityInBaseAsset ?? 0m,
                    Timestamp: DateTime.UtcNow));
                CleanupOrderState(cid);
            }
            else if (order.Status == SharedOrderStatus.Canceled && prevStatus != SharedOrderStatus.Canceled)
            {
                OrderCancelled?.Invoke(new OrderCancelledEvent(
                    Exchange, cid, order.OrderId, order.SharedSymbol!, DateTime.UtcNow));
                CleanupOrderState(cid);
            }
        }
        return Task.CompletedTask;
    }

    private Task HandleTradeUpdate(UserDataUpdate<SharedUserTrade[]> update)
    {
        foreach (var trade in update.Data)
            TryEmitOrderFilled(trade, trade.SharedSymbol!);
        return Task.CompletedTask;
    }

    // Emits OrderFilled for a SharedUserTrade exactly once per trade.Id, regardless of
    // which channel delivered it (Trades.OnUpdate or Orders.OnUpdate via LastTrade).
    private void TryEmitOrderFilled(SharedUserTrade trade, SharedSymbol symbol)
    {
        var cid = trade.ClientOrderId ?? trade.OrderId;
        decimal cumulative;
        lock (_stateLock)
        {
            if (!_seenTradeIdsByOrder.TryGetValue(cid, out var seen))
                _seenTradeIdsByOrder[cid] = seen = new HashSet<string>();
            if (!seen.Add(trade.Id)) return; // duplicate from the other channel
            cumulative = _cumulativeByOrder.GetValueOrDefault(cid) + trade.Quantity;
            _cumulativeByOrder[cid] = cumulative;
        }

        OrderFilled?.Invoke(new OrderFilledEvent(
            Exchange, cid, trade.OrderId, trade.Id, symbol,
            Side: trade.Side ?? SharedOrderSide.Buy,
            FillPrice: trade.Price,
            FillQuantity: trade.Quantity,
            CumulativeFilledBase: cumulative,
            Fee: trade.Fee,
            FeeAsset: trade.FeeAsset,
            Role: trade.Role,
            Timestamp: trade.Timestamp));
    }

    private void CleanupOrderState(string clientOrderId)
    {
        lock (_stateLock)
        {
            _cumulativeByOrder.Remove(clientOrderId);
            _seenTradeIdsByOrder.Remove(clientOrderId);
            // Keep _lastStatus so duplicate terminal updates (e.g. REST poll arriving
            // after the ws push) do not re-fire OrderCompleted / OrderCancelled.
        }
    }

    private Task HandleBalanceUpdate(UserDataUpdate<SharedBalance[]> update)
    {
        foreach (var b in update.Data)
            BalanceUpdated?.Invoke(new BalanceUpdatedEvent(
                Exchange, b.Asset, b.Available, b.Total, DateTime.UtcNow));
        return Task.CompletedTask;
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        // Prime ExchangeSymbolCache via the Shared symbols endpoint. After this call
        // returns, every SharedSpotOrder pushed by the tracker has its SharedSymbol
        // populated by the library - no need to reconstruct it on the strategy side.
        // Same payload also gives us the trading rules (min qty, tick size, ...).
        var symbols = await _symbolClient.GetSpotSymbolsAsync(new GetSymbolsRequest(), ct).ConfigureAwait(false);
        if (!symbols.Success)
            throw new InvalidOperationException($"GetSpotSymbolsAsync for {Exchange} failed: {symbols.Error}");
        foreach (var s in symbols.Data) _tradingRules[s.Name] = s;

        var trackerResult = await _tracker.StartAsync().ConfigureAwait(false);
        if (!trackerResult.Success)
            throw new InvalidOperationException($"Tracker for {Exchange} failed to start: {trackerResult.Error}");

        // Spin up a locally-synced order book for each tracked symbol.
        foreach (var symbol in _tracker.TrackedSymbols)
        {
            // minimalDepth: 10 matches CryptoManager.Net.Subscriptions.OrderBook -
            // top-10 levels is plenty for a GetPrice(symbol, isBuy) facade.
            var book = _bookFactory.Create(Exchange, symbol, minimalDepth: 10)
                ?? throw new InvalidOperationException($"No order book factory for {Exchange}/{symbol}");
            var bookStart = await book.StartAsync(ct).ConfigureAwait(false);
            if (!bookStart.Success)
                throw new InvalidOperationException($"Book for {symbol} failed to start: {bookStart.Error}");
            _books[BookKey(symbol)] = book;
        }
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        foreach (var book in _books.Values)
            try { await book.StopAsync().ConfigureAwait(false); }
            catch (Exception ex) { _logger.LogWarning(ex, "Book stop failed"); }
        _books.Clear();
        await _tracker.StopAsync().ConfigureAwait(false);
    }
}
