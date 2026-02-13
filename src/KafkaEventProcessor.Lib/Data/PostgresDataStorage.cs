using System.Runtime.CompilerServices;
using KafkaEventProcessor.Lib.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace KafkaEventProcessor.Lib.Data;

internal sealed class PostgresDataStorage : IDataStorage, IDisposable, IAsyncDisposable
{
    private readonly ILogger<PostgresDataStorage> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly int _batchSize;

    public PostgresDataStorage(
        ILogger<PostgresDataStorage> logger,
        IOptions<DataStorageOptions> dataStorageOptions,
        IConfiguration configuration)
    {
        _logger = logger;
        var connectionString = configuration["POSTGRES_CONNECTION_STRING"] ?? "";
        _dataSource = NpgsqlDataSource.Create(connectionString);
        _batchSize = dataStorageOptions.Value.BatchSize;
    }

    public async Task Initialize(
        CancellationToken cancellationToken = default)
    {
        const string createTableSql =
            """
            CREATE TABLE IF NOT EXISTS user_event_stats (
                user_id INT NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                count INT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
                PRIMARY KEY (user_id, event_type)
            );
            """;

        try
        {
            _logger.LogInformation("Initializing PostgreSQL database...");

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(createTableSql, connection);
            _ = await command.ExecuteNonQueryAsync(cancellationToken);

            _logger.LogInformation("PostgreSQL database initialized successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize PostgreSQL database");
            throw;
        }
    }

    public async IAsyncEnumerable<UserEventStats> GetStats(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        const string selectSql =
            """
            SELECT user_id, event_type, count
            FROM user_event_stats
            ORDER BY user_id, event_type;
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(selectSql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            yield return new UserEventStats
            {
                UserId = reader.GetInt32(0),
                EventType = await reader.GetFieldValueAsync<string>(1),
                Count = reader.GetInt32(2),
            };
        }
    }

    public async Task SaveStats(
        IEnumerable<UserEventStats> stats,
        CancellationToken cancellationToken = default)
    {
        const string upsertSql =
            """
            INSERT INTO user_event_stats (user_id, event_type, count, updated_at)
            VALUES (@userId, @eventType, @count, now() AT TIME ZONE 'utc')
            ON CONFLICT (user_id, event_type)
            DO UPDATE SET count = user_event_stats.count + EXCLUDED.count,
                          updated_at = now() AT TIME ZONE 'utc';
            """;

        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            try
            {
                foreach (var statsBatch in stats.Chunk(_batchSize))
                {
                    await using var batch = new NpgsqlBatch(connection, transaction);

                    foreach (var stat in statsBatch)
                    {
                        var command = new NpgsqlBatchCommand(upsertSql)
                        {
                            Parameters =
                            {
                                new NpgsqlParameter("userId", stat.UserId),
                                new NpgsqlParameter("eventType", stat.EventType),
                                new NpgsqlParameter("count", stat.Count),
                            }
                        };
                        batch.BatchCommands.Add(command);
                    }

                    _ = await batch.ExecuteNonQueryAsync(cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save statistics to PostgreSQL");
            throw;
        }
    }

    public void Dispose()
    {
        _dataSource.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _dataSource.DisposeAsync();
    }
}
