using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Confluent.Kafka;

namespace KafkaEventProcessor.Worker;

internal sealed class JsonDeserializer<T> : IDeserializer<T> where T : notnull
{
    private readonly JsonTypeInfo<T> _typeInfo;

    public JsonDeserializer(JsonTypeInfo<T> typeInfo)
    {
        _typeInfo = typeInfo;
    }

    public JsonDeserializer(JsonSerializerOptions options) : this(CreateJsonTypeInfo(options))
    {
    }

    public JsonDeserializer() : this(JsonSerializerOptions.Default)
    {
    }

    private static JsonTypeInfo<T> CreateJsonTypeInfo(JsonSerializerOptions options)
    {
        return options.TypeInfoResolver?.GetTypeInfo(typeof(T), options) as JsonTypeInfo<T> ??
               throw new InvalidOperationException($"Cannot create JsonTypeInfo<T> for type '{typeof(T).FullName}'.");
    }

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
        {
            throw new InvalidOperationException("Cannot deserialize a null message key or value.");
        }

        return JsonSerializer.Deserialize(data, _typeInfo) ??
               throw new InvalidOperationException("Deserialized message key or value is null.");
    }
}
