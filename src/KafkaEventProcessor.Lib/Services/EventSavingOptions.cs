namespace KafkaEventProcessor.Lib.Services;

public sealed class EventSavingOptions
{
    public int BatchSize { get; set; }
    public TimeSpan TimerPeriod { get; set; }
    public EventSavingMode Mode { get; set; }
}
