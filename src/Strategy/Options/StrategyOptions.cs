namespace Strategy.Options;

public sealed class StrategyOptions
{
    public const string SectionName = "Strategy";

    public string BaseAsset { get; set; } = null!;
    public string QuoteAsset { get; set; } = null!;

    public decimal Quantity { get; set; } // order size in BaseAsset
    public int CancelAfterSeconds { get; set; } // life of a resting order
    public decimal PriceOffsetPercent  { get; set; } // limit price distance from mid
}
