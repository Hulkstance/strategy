using System.Collections.Concurrent;
using CryptoExchange.Net.SharedApis;

namespace Strategy.Framework;

// JKorf's SpotOrderTracker._store is only mutated by its poll/ws handlers - there is
// no public Add() to inject a freshly-placed order. Hummingbot solves this with
// ClientOrderTracker.start_tracking_order(), which makes the order visible to the
// strategy the instant connector.buy() returns, before any exchange ack arrives.
//
// This shim sits in front of the tracker and provides the same property:
//
//   1. On placement: insert a pending SharedSpotOrder keyed by clientOrderId.
//   2. On tracker update: if the tracker now reports the same clientOrderId,
//      drop our pending copy - the tracker's copy is authoritative.
//   3. GetOpenOrders() merges tracker.Values with anything still pending here.
internal sealed class OptimisticOrderCache
{
    private readonly ConcurrentDictionary<string, SharedSpotOrder> _pending = new();

    public void AddPending(SharedSpotOrder order)
    {
        if (string.IsNullOrEmpty(order.ClientOrderId)) return;
        _pending[order.ClientOrderId!] = order;
    }

    public void RemovePending(string clientOrderId) =>
        _pending.TryRemove(clientOrderId, out _);

    public SharedSpotOrder? GetPending(string clientOrderId) =>
        _pending.TryGetValue(clientOrderId, out var o) ? o : null;

    public void ReconcileFromTracker(IEnumerable<SharedSpotOrder> trackerOrders)
    {
        foreach (var o in trackerOrders)
        {
            if (!string.IsNullOrEmpty(o.ClientOrderId))
                _pending.TryRemove(o.ClientOrderId!, out _);
        }
    }

    public IEnumerable<SharedSpotOrder> PendingOrders => _pending.Values;
}
