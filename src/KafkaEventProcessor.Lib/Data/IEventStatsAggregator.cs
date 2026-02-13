using KafkaEventProcessor.Lib.Models;

namespace KafkaEventProcessor.Lib.Data;

public interface IEventStatsAggregator
{
    void ProcessEvent(UserEvent userEvent);
    IReadOnlyCollection<UserEventStats> GetCurrentStats();
    void Remove(IReadOnlyCollection<UserEventStats> stats);
}
