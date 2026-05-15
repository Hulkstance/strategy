namespace Strategy.Options;

public sealed class BybitConnectionOptions
{
    public const string SectionName = "Bybit";

    public string ApiKey { get; set; } = "";
    public string ApiSecret { get; set; } = "";
    public string Environment { get; set; } = "";
}
