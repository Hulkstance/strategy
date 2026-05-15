using CryptoExchange.Net.SharedApis;
using CryptoExchange.Net.Trackers.UserData.Objects;

namespace Strategy.Framework;

// Strategy-facing event payloads. The connector translates raw tracker updates
// (status transitions on SharedSpotOrder) into these discrete events, the same
// way Hummingbot demultiplexes raw exchange messages into `did_*` callbacks.

public sealed record OrderCreatedEvent(
    string Exchange,
    string ClientOrderId,
    string? ExchangeOrderId,
    SharedSymbol Symbol,
    SharedOrderSide Side,
    SharedOrderType OrderType,
    decimal? Price,
    SharedQuantity? Quantity,
    DateTime Timestamp);

// Fires for every match. Sourced from the per-trade stream (tracker.Trades.OnUpdate)
// - same as CryptoManager.Net and Hummingbot's did_fill_order. Carries per-trade
// fee, role (maker/taker), and exchange trade id.
public sealed record OrderFilledEvent(
    string Exchange,
    string ClientOrderId,
    string ExchangeOrderId,
    string TradeId,
    SharedSymbol Symbol,
    SharedOrderSide Side,
    decimal FillPrice,
    decimal FillQuantity,
    decimal CumulativeFilledBase,
    decimal? Fee,
    string? FeeAsset,
    SharedRole? Role,
    DateTime Timestamp);

// Fires once when an order transitions to Status == Filled (fully filled).
// Hummingbot's did_complete_buy_order / did_complete_sell_order; mirrors the
// "this order is done, no more fills coming" semantic.
public sealed record OrderCompletedEvent(
    string Exchange,
    string ClientOrderId,
    string ExchangeOrderId,
    SharedSymbol Symbol,
    SharedOrderSide Side,
    decimal AveragePrice,
    decimal TotalFilledBase,
    DateTime Timestamp);

public sealed record OrderCancelledEvent(
    string Exchange,
    string ClientOrderId,
    string ExchangeOrderId,
    SharedSymbol Symbol,
    DateTime Timestamp);

public sealed record OrderRejectedEvent(
    string Exchange,
    string ClientOrderId,
    SharedSymbol Symbol,
    string Reason,
    DateTime Timestamp);

public sealed record BalanceUpdatedEvent(
    string Exchange,
    string Asset,
    decimal Available,
    decimal Total,
    DateTime Timestamp);

// Stream connection status. CryptoManager.Net emits StreamStatus.Restored/Interrupted
// per-stream; we forward the same signal so strategies can pause/resume on disconnect.
public sealed record StreamStatusEvent(
    string Exchange,
    UserDataType Stream,  // Balances | Orders | Trades | Positions
    bool Connected,
    DateTime Timestamp);
