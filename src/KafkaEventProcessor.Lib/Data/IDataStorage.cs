using KafkaEventProcessor.Lib.Models;

namespace KafkaEventProcessor.Lib.Data;

public interface IDataStorage
{
    Task Initialize(CancellationToken cancellationToken = default);
    IAsyncEnumerable<UserEventStats> GetStats(CancellationToken cancellationToken = default);
    Task SaveStats(IEnumerable<UserEventStats> stats, CancellationToken cancellationToken = default);
}
