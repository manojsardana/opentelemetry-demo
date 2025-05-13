// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
using System;

using Grpc.HealthCheck;
using Grpc.Health.V1;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using System.Threading.Tasks;
using System.Threading;

using Grpc.Core;

using cart.cartstore;
using cart.services;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Instrumentation.StackExchangeRedis;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenFeature;
using OpenFeature.Contrib.Providers.Flagd;
using OpenFeature.Contrib.Hooks.Otel;

var builder = WebApplication.CreateBuilder(args);
var isReady = true;
string valkeyAddress = builder.Configuration["VALKEY_ADDR"];
if (string.IsNullOrEmpty(valkeyAddress))
{
    Console.WriteLine("VALKEY_ADDR environment variable is required.");
    Environment.Exit(1);
}

builder.Logging
    .AddOpenTelemetry(options => options.AddOtlpExporter())
    .AddConsole();

builder.Services.AddSingleton<ICartStore>(x=>
{
    var store = new ValkeyCartStore(x.GetRequiredService<ILogger<ValkeyCartStore>>(), valkeyAddress);
    store.Initialize();
    return store;
});

builder.Services.AddSingleton<IFeatureClient>(x => {
    var flagdProvider = new FlagdProvider();
    Api.Instance.SetProviderAsync(flagdProvider).GetAwaiter().GetResult();
    var client = Api.Instance.GetClient();
    return client;
});

builder.Services.AddSingleton(x =>
    new CartService(
        x.GetRequiredService<ICartStore>(),
        new ValkeyCartStore(x.GetRequiredService<ILogger<ValkeyCartStore>>(), "badhost:1234"),
        x.GetRequiredService<IFeatureClient>()
));


Action<ResourceBuilder> appResourceBuilder =
    resource => resource
        .AddContainerDetector()
        .AddHostDetector();

builder.Services.AddOpenTelemetry()
    .ConfigureResource(appResourceBuilder)
    .WithTracing(tracerBuilder => tracerBuilder
        .AddSource("OpenTelemetry.Demo.Cart")
        .AddRedisInstrumentation(
            options => options.SetVerboseDatabaseStatements = true)
        .AddAspNetCoreInstrumentation()
        .AddGrpcClientInstrumentation()
        .AddHttpClientInstrumentation()
        .AddOtlpExporter())
    .WithMetrics(meterBuilder => meterBuilder
        .AddMeter("OpenTelemetry.Demo.Cart")
        .AddProcessInstrumentation()
        .AddRuntimeInstrumentation()
        .AddAspNetCoreInstrumentation()
        .SetExemplarFilter(ExemplarFilterType.TraceBased)
        .AddOtlpExporter());
OpenFeature.Api.Instance.AddHooks(new TracingHook());
builder.Services.AddGrpc();
builder.Services.AddHttpContextAccessor();
builder.Services.AddGrpcHealthChecks()
    .AddCheck<readinessCheck>("oteldemo.CartService");

builder.Services.AddSingleton<HealthServiceImpl>(); 
//builder.Services.AddHttpContextAccessor();

var app = builder.Build();

var ValkeyCartStore = (ValkeyCartStore) app.Services.GetRequiredService<ICartStore>();
app.Services.GetRequiredService<StackExchangeRedisInstrumentation>().AddConnection(ValkeyCartStore.GetConnection());


//var grpcHealth = app.Services.GetRequiredService<HealthServiceImpl>();

// Set gRPC health status for your service and global ("" is the global key)
//grpcHealth.SetStatus("oteldemo.CartService", HealthCheckResponse.Types.ServingStatus.Serving);
//grpcHealth.SetStatus(string.Empty, HealthCheckResponse.Types.ServingStatus.Serving);

app.MapGrpcService<CartService>();
//app.MapGrpcService<Health.HealthBase, HealthServiceImpl>();
app.MapGrpcService<HealthServiceImpl>();
//app.MapGrpcHealthChecksService();

// AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

app.MapGet("/", async context =>
{
    await context.Response.WriteAsync("Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
});



// A healthy “ping” you can use to verify the app is up
app.MapGet("/health", () => Results.Text("OK"));

app.Run();


public class readinessCheck : IHealthCheck
{
        private readonly IHttpContextAccessor _httpContextAccessor;

        public readinessCheck(IHttpContextAccessor httpContextAccessor)
        {
            _httpContextAccessor = httpContextAccessor;
        }
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var httpContext = _httpContextAccessor.HttpContext;
        // Implement your database connectivity check here
      var featureClient = httpContext.RequestServices.GetRequiredService<IFeatureClient>();
    
    // Await the async call instead of blocking
     //  bool failed = 
        bool isSet = await featureClient.GetBooleanValueAsync("failedReadinessProbe", false); // Replace with actual check

        if (isSet)
        {
            return HealthCheckResult.Unhealthy("connection failed");
            
        }

        return HealthCheckResult.Healthy("healthy");
    }
}


    public class HealthServiceImpl : Health.HealthBase
    {
        private readonly ILogger<HealthServiceImpl> _logger;
        private readonly HealthCheckService _healthCheckService;


        public HealthServiceImpl(
            ILogger<HealthServiceImpl> logger,
            HealthCheckService healthCheckService)
        {
            _logger = logger;
            _healthCheckService = healthCheckService;
        }

        public override async Task<HealthCheckResponse> Check(HealthCheckRequest request, ServerCallContext context)
        {
            _logger.LogInformation("Received health check request for service: {Service}", request.Service);
             var cancellationToken = context.CancellationToken;
            // If service is empty or null, check overall health
            if (string.IsNullOrEmpty(request.Service))
            {
                var health = await _healthCheckService.CheckHealthAsync(cancellationToken);
                return new HealthCheckResponse
                {
                    Status = ConvertToGrpcStatus(health.Status)
                };
            }

            // You can implement service-specific health checks here
            // This example checks a specific service
            var serviceHealth = await _healthCheckService.CheckHealthAsync(registration => MatchesService(registration, request.Service),cancellationToken);
            return new HealthCheckResponse
            {
                Status = ConvertToGrpcStatus(serviceHealth.Entries[request.Service].Status)
            };
        }

        private bool MatchesService(HealthCheckRegistration registration, string service)
        {
            return registration.Name == service;
        }
        public override async Task Watch(HealthCheckRequest request, IServerStreamWriter<HealthCheckResponse> responseStream, ServerCallContext context)
        {
            _logger.LogInformation("Received health watch request for service: {Service}", request.Service);

            // Simple implementation to send current status once
            var response = await Check(request, context);
            await responseStream.WriteAsync(response);

            // In a real implementation, you would periodically check health and send updates
            // This might involve setting up a timer or listener for health changes
        }

        private static HealthCheckResponse.Types.ServingStatus ConvertToGrpcStatus(HealthStatus status)
        {
            return status switch
            {
                HealthStatus.Healthy => HealthCheckResponse.Types.ServingStatus.Serving,
                HealthStatus.Degraded => HealthCheckResponse.Types.ServingStatus.Serving, // Or you might want to use SERVING_WITH_ISSUES if available
                HealthStatus.Unhealthy => HealthCheckResponse.Types.ServingStatus.NotServing,
                _ => HealthCheckResponse.Types.ServingStatus.Unknown
            };
        }
    }


