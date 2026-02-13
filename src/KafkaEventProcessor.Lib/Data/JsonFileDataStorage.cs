using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using KafkaEventProcessor.Lib.Models;
using Microsoft.Extensions.Logging;

namespace KafkaEventProcessor.Lib.Data;

internal sealed class JsonFileDataStorage : IDataStorage
{
    private readonly ILogger<JsonFileDataStorage> _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly string _filePath;
    private readonly SemaphoreSlim _fileLock = new(1, 1);

    public JsonFileDataStorage(
        ILogger<JsonFileDataStorage> logger,
        JsonSerializerOptions jsonOptions,
        string? filePath = null)
    {
        _logger = logger;
        _jsonOptions = jsonOptions;
        _filePath = filePath ?? Path.Combine(Directory.GetCurrentDirectory(), "user_event_stats.json");
    }

    public async Task Initialize(
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Initializing JSON file storage...");

            var directory = Path.GetDirectoryName(_filePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            if (!File.Exists(_filePath))
            {
                await File.WriteAllTextAsync(_filePath, "[]", cancellationToken);
                _logger.LogInformation("JSON file storage initialized successfully");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize JSON file storage");
            throw;
        }
    }

    public async IAsyncEnumerable<UserEventStats> GetStats(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await _fileLock.WaitAsync(cancellationToken);

        try
        {
            await using var file = File.OpenRead(_filePath);
            var stats = JsonSerializer.DeserializeAsyncEnumerable<UserEventStats>(file, _jsonOptions, cancellationToken);
            await foreach (var stat in stats)
            {
                if (stat is null)
                {
                    continue;
                }

                yield return stat;
            }
        }
        finally
        {
            _fileLock.Release();
        }
    }

    public async Task SaveStats(
        IEnumerable<UserEventStats> stats,
        CancellationToken cancellationToken = default)
    {
        await _fileLock.WaitAsync(cancellationToken);

        try
        {
            await using var file = File.Open(_filePath, FileMode.Open, FileAccess.ReadWrite);
            var existingStats = await JsonSerializer.DeserializeAsync<List<UserEventStats>>(file, _jsonOptions, cancellationToken) ?? [];
            var statsDict = existingStats.ToDictionary(s => (s.UserId, s.EventType), s => s.Count);

            foreach (var stat in stats)
            {
                var key = (stat.UserId, stat.EventType);
                ref var countRef = ref CollectionsMarshal.GetValueRefOrAddDefault(statsDict, key, out _);
                countRef += stat.Count;
            }

            var updatedStats = statsDict.Select(kvp => new UserEventStats
            {
                UserId = kvp.Key.UserId,
                EventType = kvp.Key.EventType,
                Count = kvp.Value
            }).OrderBy(s => s.UserId).ThenBy(s => s.EventType);

            file.Seek(0, SeekOrigin.Begin);
            await JsonSerializer.SerializeAsync(file, updatedStats, _jsonOptions, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save statistics to JSON file storage");
            throw;
        }
        finally
        {
            _fileLock.Release();
        }
    }
}
