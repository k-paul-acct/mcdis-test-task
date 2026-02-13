using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS"),
};

var topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC");

using var producer = new ProducerBuilder<Null, string>(config).Build();

Console.WriteLine("Producing starting...");

using var cts = SetupAppExit();
var cancellationToken = cts.Token;
var count = 0;

try
{
    var options = new JsonSerializerOptions() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };
    while (!cancellationToken.IsCancellationRequested)
    {
        var eventObj = GenerateEvent();
        var json = JsonSerializer.Serialize(eventObj, options);

        producer.Produce(topic, new Message<Null, string> { Value = json });

        if (++count % 10 == 0)
        {
            producer.Flush(cancellationToken);
        }

        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
catch (OperationCanceledException)
{
}
finally
{
    producer.Flush(TimeSpan.FromSeconds(5));
    Console.WriteLine($"\nEvents produced: {count}.");
}

return;

static object GenerateEvent()
{
    string[] eventTypes = ["click", "hover", "hold"];
    string[] buttonIds = ["submit", "cancel", "save", "delete", "edit", "close"];

    return new
    {
        userId = Random.Shared.Next(1, 1001),
        eventType = eventTypes[Random.Shared.Next(eventTypes.Length)],
        timestamp = DateTime.UtcNow.ToString("O"),
        data = Random.Shared.NextDouble() < 0.5
            ? new { buttonId = buttonIds[Random.Shared.Next(buttonIds.Length)] }
            : null,
    };
}

static CancellationTokenSource SetupAppExit()
{
    var cancellationTokenSource = new CancellationTokenSource();
    var cancelled = false;

    Console.CancelKeyPress += (_, ea) =>
    {
        ea.Cancel = true;
        cancelled = true;
        cancellationTokenSource.Cancel();
    };

    AppDomain.CurrentDomain.ProcessExit += (_, _) =>
    {
        if (!cancelled)
        {
            cancellationTokenSource.Cancel();
        }
    };

    return cancellationTokenSource;
}
