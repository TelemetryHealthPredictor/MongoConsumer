using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using MongoConsumer.Config;
using MongoConsumer.Services;
using ParameterDataLib;

namespace MongoConsumer.Build
{
    internal class KafkaConsumerService : BackgroundService
    {
        private static readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(2);

        private readonly BatchHandler _batchHandler;
        private readonly KafkaConsumerConfig _config;
        private readonly IHostApplicationLifetime _lifetime;

        public KafkaConsumerService
        (
            BatchHandler batchHandler,
            IOptions<KafkaConsumerConfig> options,
            IHostApplicationLifetime lifetime
        )
        {
            _batchHandler = batchHandler;
            _config = options.Value;
            _lifetime = lifetime;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => RunAsync(stoppingToken), stoppingToken);
        }

        private async Task RunAsync(CancellationToken ct)
        {
            ConsumerConfig consumerConfig = new()
            {
                BootstrapServers = _config.BootstrapServers,
                GroupId = _config.GroupId,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoOffsetStore = false
            };

            using IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

            try
            {
                consumer.Subscribe(_config.TopicName);

                while (!ct.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result;

                    try
                    {
                        result = consumer.Consume(ct);
                    }
                    catch (ConsumeException ex) when (!ex.Error.IsFatal)
                    {
                        Console.WriteLine($"Kafka consume error: {ex.Error.Reason}");
                        Console.WriteLine(ex);
                        await Task.Delay(_retryDelay, ct);
                        continue;
                    }

                    if (result?.Message?.Value is null)
                    {
                        continue;
                    }

                    ParameterData[]? batch;

                    try
                    {
                        batch = JsonSerializer.Deserialize<ParameterData[]>(result.Message.Value);
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine($"Skipping malformed message at {result.TopicPartitionOffset}");
                        Console.WriteLine(ex);
                        consumer.StoreOffset(result);
                        continue;
                    }

                    if (batch is null || batch.Length == 0)
                    {
                        Console.WriteLine($"Skipping empty message at {result.TopicPartitionOffset}");
                        consumer.StoreOffset(result);
                        continue;
                    }

                    await _batchHandler.ProcessAsync(batch, ct);
                    consumer.StoreOffset(result);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Normal shutdown.
            }
            catch (Exception ex)
            {
                Console.WriteLine("Kafka consume loop failed; stopping the application.");
                Console.WriteLine(ex);
                Environment.ExitCode = 1;
                _lifetime.StopApplication();
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}