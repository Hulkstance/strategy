using CryptoExchange.Net.SharedApis;

namespace Strategy.Framework;

// Hummingbot-style facade: synchronous cache reads, async order placement,
// event hooks for state transitions. A strategy depends only on this interface -
// it never sees IUserSpotDataTracker, ISpotOrderRestClient, or any JKorf type directly.
public interface IConnector
{
    string Exchange { get; }
    bool Ready { get; }

    // ---- Synchronous cache reads (no REST in the hot path) ----
    decimal GetBalance(string asset);
    decimal GetAvailableBalance(string asset);
    IReadOnlyCollection<SharedSpotOrder> GetOpenOrders();
    SharedSpotOrder? GetOrder(string clientOrderId);
    SharedSpotSymbol? GetTradingRules(SharedSymbol symbol);

    // Hummingbot's connector.get_price(pair, is_buy): isBuy=true returns the ask
    // (price to cross to buy), isBuy=false returns the bid. Reads from a locally-synced
    // SymbolOrderBook fed by websocket + snapshot. Returns null until the book is ready.
    decimal? GetPrice(SharedSymbol symbol, bool isBuy);

    // ---- Async order actions ----
    Task<string> BuyAsync(
        SharedSymbol symbol,
        SharedOrderType orderType,
        SharedQuantity quantity,
        decimal? price = null,
        SharedTimeInForce? timeInForce = null,
        CancellationToken ct = default);

    Task<string> SellAsync(
        SharedSymbol symbol,
        SharedOrderType orderType,
        SharedQuantity quantity,
        decimal? price = null,
        SharedTimeInForce? timeInForce = null,
        CancellationToken ct = default);

    Task CancelAsync(string clientOrderId, CancellationToken ct = default);

    // ---- Event hooks ----
    event Action<OrderCreatedEvent>?   OrderCreated;
    event Action<OrderFilledEvent>?    OrderFilled;     // per match, partial OR final
    event Action<OrderCompletedEvent>? OrderCompleted;  // once, on Open -> Filled
    event Action<OrderCancelledEvent>? OrderCancelled;
    event Action<OrderRejectedEvent>?  OrderRejected;
    event Action<BalanceUpdatedEvent>? BalanceUpdated;
    event Action<StreamStatusEvent>?   StreamStatusChanged;  // websocket connect/disconnect

    Task StartAsync(CancellationToken ct = default);
    Task StopAsync(CancellationToken ct = default);
}
