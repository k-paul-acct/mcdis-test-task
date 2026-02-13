using KafkaEventProcessor.Lib.Data;
using KafkaEventProcessor.Lib.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaEventProcessor.Lib.Services;

public sealed class EventObserver : IObserver<UserEvent>, IDisposable
{
    private readonly ILogger<EventObserver> _logger;
    private readonly IDataStorage _dataStorage;
    private readonly IEventStatsAggregator _statsAggregator;
    private readonly Timer? _saveTimer;
    private IDisposable? _subscription;
    private readonly SemaphoreSlim _saveSemaphore = new(1, 1);
    private readonly int _batchSize;
    private uint _eventCount;

    public EventObserver(
        ILogger<EventObserver> logger,
        IDataStorage dataStorage,
        IEventStatsAggregator statsAggregator,
        IOptions<EventSavingOptions> options)
    {
        _logger = logger;
        _dataStorage = dataStorage;
        _statsAggregator = statsAggregator;

        if ((options.Value.Mode & EventSavingMode.TimerBased) != 0)
        {
            var period = options.Value.TimerPeriod;
            _saveTimer = new Timer(_ => Task.Run(SaveStats), state: null, period, period);
        }

        _batchSize = (options.Value.Mode & EventSavingMode.BatchBased) != 0
            ? options.Value.BatchSize
            : int.MaxValue;
    }

    public void SetSubscription(IDisposable subscription)
    {
        _subscription = subscription;
    }

    public void OnNext(UserEvent value)
    {
        _statsAggregator.ProcessEvent(value);
        var newCount = Interlocked.Increment(ref _eventCount);
        if (newCount % _batchSize == 0)
        {
            _ = Task.Run(SaveStats);
        }
    }

    public void OnError(Exception error)
    {
        _logger.LogError(error, "Error occurred in the event stream");
    }

    public void OnCompleted()
    {
        _logger.LogInformation("Event stream completed. Saving pending statistics...");
        SaveStats().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _saveTimer?.Dispose();
        _subscription?.Dispose();
        SaveStats().GetAwaiter().GetResult();
        _saveSemaphore.Dispose();
    }

    private async Task SaveStats()
    {
        await _saveSemaphore.WaitAsync();

        try
        {
            var stats = _statsAggregator.GetCurrentStats();
            if (stats.Count == 0)
            {
                return;
            }

            await _dataStorage.SaveStats(stats);
            _statsAggregator.Remove(stats);
            _logger.LogInformation("Saved statistics batch");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while saving statistics");
        }
        finally
        {
            _saveSemaphore.Release();
        }
    }
}
