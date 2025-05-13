// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Oteldemo;
using Npgsql;
using System;
using System.Data;
using System.Text.Json;

namespace Accounting;

internal class Consumer : IDisposable
{
    private const string TopicName = "orders";
    private string _connectionString;

    private ILogger _logger;
    private IConsumer<string, byte[]> _consumer;
    private bool _isListening;

    public Consumer(ILogger<Consumer> logger)
    {
        _logger = logger;

        var servers = Environment.GetEnvironmentVariable("KAFKA_ADDR")
            ?? throw new ArgumentNullException("KAFKA_ADDR");
        _connectionString = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");

        _consumer = BuildConsumer(servers);
        _consumer.Subscribe(TopicName);

        _logger.LogInformation($"Connecting to Kafka: {servers}");
    }

    public void StartListening()
    {
        _isListening = true;

        try
        {
            while (_isListening)
            {
                try
                {
                    var consumeResult = _consumer.Consume();

                    ProcessMessage(consumeResult.Message);
                    System.Threading.Thread.Sleep(10);
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Consume error: {0}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Closing consumer");

            _consumer.Close();
        }
    }

    private void ProcessMessage(Message<string, byte[]> message)
    {
        try
        {
            var order = OrderResult.Parser.ParseFrom(message.Value);

            Log.OrderReceivedMessage(_logger, order);

            SaveOrderToDatabase(order, message.Key);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Order parsing failed:");
        }
    }

    private void SaveOrderToDatabase(OrderResult order, string messageKey)
    {
        try
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();

                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = @"
                        INSERT INTO orders (
                            order_id, 
                            shipping_tracking_id, 
                            shipping_cost_amount,
                            shipping_cost_currency,
                            shipping_address, 
                            items, 
                            message_key, 
                            created_at
                        ) VALUES (
                            @orderId, 
                            @shippingTrackingId, 
                            @shippingCostAmount, 
                            @shippingCostCurrency, 
                            @shippingAddress, 
                            @items, 
                            @messageKey, 
                            @createdAt
                        )";

                    // Add parameters to prevent SQL injection
                    cmd.Parameters.AddWithValue("@orderId", order.OrderId);
                    cmd.Parameters.AddWithValue("@shippingTrackingId", order.ShippingTrackingId);
                    
                    // Handle Money type for shipping_cost
                    double shippingCost = order.ShippingCost.Units + (order.ShippingCost.Nanos / 1_000_000_000.0);
                    cmd.Parameters.AddWithValue("@shippingCostAmount", shippingCost);
                    cmd.Parameters.AddWithValue("@shippingCostCurrency", order.ShippingCost?.CurrencyCode ?? "");
                    
                    // Serialize Address to JSON
                    var addressJson = JsonSerializer.Serialize(order.ShippingAddress);
                    cmd.Parameters.AddWithValue("@shippingAddress", addressJson);
                    
                    // Serialize items collection to JSON
                    var itemsJson = JsonSerializer.Serialize(order.Items);
                    cmd.Parameters.AddWithValue("@items", itemsJson);
                    
                      if (messageKey != null)
                    {
                        cmd.Parameters.AddWithValue("@messageKey", messageKey);
                    }
                    else
                    {
                        cmd.Parameters.AddWithValue("@messageKey", DBNull.Value);
                    }
                    cmd.Parameters.AddWithValue("@createdAt", DateTime.UtcNow);

                    cmd.ExecuteNonQuery();
                    
                    _logger.LogInformation($"Order {order.OrderId} saved to PostgreSQL database");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save order to PostgreSQL:");
        }
    }

    private IConsumer<string, byte[]> BuildConsumer(string servers)
    {
        var conf = new ConsumerConfig
        {
            GroupId = $"accounting",
            BootstrapServers = servers,
            // https://github.com/confluentinc/confluent-kafka-dotnet/tree/07de95ed647af80a0db39ce6a8891a630423b952#basic-consumer-example
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        return new ConsumerBuilder<string, byte[]>(conf)
            .Build();
    }

    public void Dispose()
    {
        _isListening = false;
        _consumer?.Dispose();
    }
}
