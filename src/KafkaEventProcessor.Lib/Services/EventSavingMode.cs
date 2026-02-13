namespace KafkaEventProcessor.Lib.Services;

[Flags]
public enum EventSavingMode
{
    BatchBased = 1,
    TimerBased = 2,
    BatchOrTimerBased = BatchBased | TimerBased,
}
