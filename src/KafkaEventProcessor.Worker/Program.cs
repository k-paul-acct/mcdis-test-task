using KafkaEventProcessor.Lib.Data;
using KafkaEventProcessor.Lib.Extensions;
using KafkaEventProcessor.Lib.Services;
using KafkaEventProcessor.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddKafkaEventProcessorServices(builder.Configuration)
    .AddHostedService<KafkaConsumerService>();

var host = builder.Build();

await using (var scope = host.Services.CreateAsyncScope())
{
    var sp = scope.ServiceProvider;

    // Data storage setup.
    var dataStorage = sp.GetRequiredService<IDataStorage>();
    await dataStorage.Initialize();

    // Observer setup.
    var eventObservable = sp.GetRequiredService<EventObservable>();
    var eventObserver = sp.GetRequiredService<EventObserver>();
    var subscription = eventObservable.Subscribe(eventObserver);
    eventObserver.SetSubscription(subscription);
}

host.Run();
