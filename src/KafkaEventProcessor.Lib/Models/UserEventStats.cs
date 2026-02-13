namespace KafkaEventProcessor.Lib.Models;

public sealed class UserEventStats
{
    public int UserId { get; init; }
    public string EventType { get; init; } = "";
    public int Count { get; init; }
}
