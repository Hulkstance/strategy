using Bybit.Net;
using CryptoClients.Net.Interfaces;
using CryptoExchange.Net.SharedApis;
using CryptoExchange.Net.Trackers.UserData.Objects;
using Strategy.Framework;
using Strategy.Options;
using Strategy.Strategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

var host = Host.CreateApplicationBuilder(args);

host.Services
    .AddOptions<BybitConnectionOptions>()
    .Bind(host.Configuration.GetSection(BybitConnectionOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.ApiKey),
        $"{BybitConnectionOptions.SectionName}:ApiKey is required")
    .Validate(o => !string.IsNullOrWhiteSpace(o.ApiSecret),
        $"{BybitConnectionOptions.SectionName}:ApiSecret is required")
    .Validate(o => BybitEnvironment.GetEnvironmentByName(o.Environment) is not null,
        $"{BybitConnectionOptions.SectionName}:Environment must be one of: " +
        string.Join(", ", BybitEnvironment.All))
    .ValidateOnStart();

host.Services
    .AddOptions<StrategyOptions>()
    .Bind(host.Configuration.GetSection(StrategyOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.BaseAsset),  $"{StrategyOptions.SectionName}:BaseAsset is required")
    .Validate(o => !string.IsNullOrWhiteSpace(o.QuoteAsset), $"{StrategyOptions.SectionName}:QuoteAsset is required")
    .Validate(o => o.Quantity            > 0m,  $"{StrategyOptions.SectionName}:Quantity must be > 0")
    .Validate(o => o.CancelAfterSeconds  > 0,   $"{StrategyOptions.SectionName}:CancelAfterSeconds must be > 0")
    .Validate(o => o.PriceOffsetPercent  > 0m,  $"{StrategyOptions.SectionName}:PriceOffsetPercent must be > 0")
    .ValidateOnStart();

host.Logging.ClearProviders();
host.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss ";
});

var bybitOpts = host.Configuration.GetSection(BybitConnectionOptions.SectionName)
    .Get<BybitConnectionOptions>()
    ?? throw new InvalidOperationException("Bybit configuration section missing");
host.Services.AddCryptoClients(
    bybitOptions: opt =>
    {
        opt.ApiCredentials = new BybitCredentials(bybitOpts.ApiKey, bybitOpts.ApiSecret);
        opt.Environment    = BybitEnvironment.GetEnvironmentByName(bybitOpts.Environment)!;
    });

host.Services.AddSingleton<IConnector>(sp =>
{
    var strategyOpts   = sp.GetRequiredService<IOptions<StrategyOptions>>().Value;
    var trackerFactory = sp.GetRequiredService<IExchangeTrackerFactory>();
    var restClient     = sp.GetRequiredService<IExchangeRestClient>();
    var bookFactory    = sp.GetRequiredService<IExchangeOrderBookFactory>();

    var symbol = new SharedSymbol(TradingMode.Spot, strategyOpts.BaseAsset, strategyOpts.QuoteAsset);

    var tracker = trackerFactory.CreateUserSpotDataTracker("Bybit", new SpotUserDataTrackerConfig
    {
        OnlyTrackProvidedSymbols = true,
        TrackedSymbols = new[] { symbol }
    })!;
    var orderClient  = restClient.GetSpotOrderClient("Bybit")!;
    var symbolClient = restClient.GetSpotSymbolClient("Bybit")!;

    return new SpotConnector(tracker, orderClient, symbolClient, bookFactory,
        sp.GetRequiredService<ILogger<SpotConnector>>());
});

host.Services.AddSingleton<StrategyHost>(sp =>
{
    var strategyOpts = sp.GetRequiredService<IOptions<StrategyOptions>>().Value;
    var strategyHost = new StrategyHost(sp.GetRequiredService<ILoggerFactory>());
    strategyHost.Register(
        sp.GetRequiredService<IConnector>(),
        new PingPongStrategy(strategyOpts));
    return strategyHost;
});

host.Services.AddHostedService<TradingClock>();

await host.Build().RunAsync();
