namespace KafkaEventProcessor.Lib.Models;

public sealed class UserEvent
{
    public int UserId { get; init; }
    public string EventType { get; init; } = "";
    public DateTimeOffset Timestamp { get; init; }
}
