using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaEventProcessor.Lib.Data;
using KafkaEventProcessor.Lib.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KafkaEventProcessor.Lib.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaEventProcessorServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddTransient<IEventStatsAggregator, EventStatsAggregator>();
        var storageTypeString = configuration["DataStorage:Type"] ?? nameof(DataStorageType.JsonFile);
        var storageType = Enum.Parse<DataStorageType>(storageTypeString);
        if (storageType == DataStorageType.Postgres)
        {
            services.Configure<DataStorageOptions>(configuration.GetSection("DataStorage"));
            services.AddSingleton<IDataStorage, PostgresDataStorage>();
        }
        else if (storageType == DataStorageType.JsonFile)
        {
            services.AddSingleton<IDataStorage, JsonFileDataStorage>(sp =>
            {
                var logger = sp.GetRequiredService<ILogger<JsonFileDataStorage>>();
                var jsonOptions = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                };
                return new JsonFileDataStorage(logger, jsonOptions, filePath: null);
            });
        }

        services.Configure<EventSavingOptions>(configuration.GetSection("EventSaving"));
        services.AddSingleton<EventObservable>();
        services.AddSingleton<EventObserver>();

        return services;
    }
}
