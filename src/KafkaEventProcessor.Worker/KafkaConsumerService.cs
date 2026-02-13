using System.Text.Json;
using Confluent.Kafka;
using KafkaEventProcessor.Lib.Models;
using KafkaEventProcessor.Lib.Services;

namespace KafkaEventProcessor.Worker;

internal sealed class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IConfiguration _configuration;
    private readonly EventObservable _observable;

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IConfiguration configuration,
        EventObservable observable)
    {
        _logger = logger;
        _configuration = configuration;
        _observable = observable;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _configuration["KAFKA_BOOTSTRAP_SERVERS"],
            GroupId = _configuration["KAFKA_GROUP_ID"],
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        var topic = _configuration["KAFKA_TOPIC"];

        var consumer = new ConsumerBuilder<Ignore, UserEvent>(config)
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError("Kafka error: {Reason}", e.Reason);
            })
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                var joinedPartitions = string.Join(", ", partitions.Select(p => p.Partition.Value));
                _logger.LogInformation("Partitions assigned: {Partitions}", joinedPartitions);
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                var joinedPartitions = string.Join(", ", partitions.Select(p => p.Partition.Value));
                _logger.LogInformation("Partitions revoked: {Partitions}", joinedPartitions);
            })
            .SetValueDeserializer(new JsonDeserializer<UserEvent>(JsonSerializerOptions.Web))
            .Build();

        try
        {
            consumer.Subscribe(topic);
            _logger.LogInformation("Subscribed to topic: {Topic}", topic);
            await ConsumeMessages(consumer, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Kafka consumer cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error");
            _observable.PublishError(ex);
            throw;
        }
        finally
        {
            Shutdown(consumer);
        }
    }

    private async Task ConsumeMessages(IConsumer<Ignore, UserEvent> consumer, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);
                _observable.PublishEvent(consumeResult.Message.Value);
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                _logger.LogError(ex, "Error while consuming message: {Reason}", ex.Error.Reason);
            }
        }
    }

    private void Shutdown(IConsumer<Ignore, UserEvent> consumer)
    {
        try
        {
            consumer.Close();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while closing Kafka consumer");
        }
        finally
        {
            consumer.Dispose();
            _observable.Complete();
        }
    }
}
