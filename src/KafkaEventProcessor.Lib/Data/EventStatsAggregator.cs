using System.Collections.Concurrent;
using KafkaEventProcessor.Lib.Models;

namespace KafkaEventProcessor.Lib.Data;

internal sealed class EventStatsAggregator : IEventStatsAggregator
{
    private readonly ConcurrentDictionary<(int UserId, string EventType), StatItem> _stats;

    public EventStatsAggregator()
    {
        _stats = new ConcurrentDictionary<(int UserId, string EventType), StatItem>();
    }

    public void ProcessEvent(UserEvent userEvent)
    {
        var key = (userEvent.UserId, userEvent.EventType);
        _stats.AddOrUpdate(
            key,
            new StatItem(userEvent.UserId, userEvent.EventType, 1),
            static (_, item) => item with { Count = item.Count + 1 });
    }

    public IReadOnlyCollection<UserEventStats> GetCurrentStats()
    {
        var items = _stats.Values
            .Where(item => item.Count > 0)
            .Select(item => new UserEventStats
            {
                UserId = item.UserId,
                EventType = item.EventType,
                Count = item.Count,
            }).ToList();

        return items;
    }

    public void Remove(IReadOnlyCollection<UserEventStats> stats)
    {
        foreach (var stat in stats)
        {
            var key = (stat.UserId, stat.EventType);
            _stats.AddOrUpdate(
                key,
                static (key, _) => new StatItem(key.UserId, key.EventType, 0),
                static (_, item, state) => item with { Count = item.Count - state },
                stat.Count);
        }
    }

    private record struct StatItem(int UserId, string EventType, int Count);
}
