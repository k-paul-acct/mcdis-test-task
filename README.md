# Kafka Event Processor

Сервис, разработанный по заданию MCDis.

### Что реализовано

- Чтение событий из Apache Kafka
- Паттерн Observer (IObservable/IObserver)
- Сохранение агрегированных данных в PostgreSQL или JSON-файл
- Batch-обработка
- Различные конфигурации запуска

### Структура проекта

```
src
├── KafkaEventProcessor.EventsGenerator      # Вспомогательный проект для генерации сообщений в Kafka
│   └── Program.cs
├── KafkaEventProcessor.Lib                  # Главная библиотека
│   ├── Data                                 # Всё для работы с данными + конфигурация
│   │   ├── DataStorageOptions.cs
│   │   ├── DataStorageType.cs
│   │   ├── EventStatsAggregator.cs
│   │   ├── IDataStorage.cs
│   │   ├── IEventStatsAggregator.cs
│   │   ├── JsonFileDataStorage.cs
│   │   └── PostgresDataStorage.cs
│   ├── Extensions
│   │   └── ServiceCollectionExtensions.cs
│   ├── Models                               # Модели данных
│   │   ├── UserEvent.cs
│   │   └── UserEventStats.cs
│   └── Services                             # Реализация паттерна + конфигурация
│       ├── EventObservable.cs
│       ├── EventObserver.cs
│       ├── EventSavingMode.cs
│       └── EventSavingOptions.cs
└── KafkaEventProcessor.Worker               # Исполняемый проект-обёртка для запуска
    ├── JsonDeserializer.cs
    ├── KafkaConsumerService.cs
    └── Program.cs
```

### Запуск

Инфраструктура для локального запуска может быть развёрнута через Docker:
```bash
docker compose up --build
```
Запускаются: Kafka, Kafka UI, PostgreSQL.

Приложения могут быть запущены как локально (`dotnet run`), так и в Docker.
Подключение к инфраструктуре происходит через переменные окружения.
По умолчанию конфигурация подразумевает запуск в Docker только для Kafka, Kafka UI и PostgreSQL. .NET-приложения запускаются локально.

Для запуска приложения в Docker:
```bash
docker compose --profile app up --build
```

#### Kafka

При запуске в Docker данные для подключения берутся из переменных окружения.

#### Kafka UI

При запуске в Docker доступен по адресу http://localhost:8080

#### PostgreSQL

Используется стандартное подключение к БД. Если БД запущена в Docker, то строка подключения с хоста будет `Host=127.0.0.1;Port=5433;Database=appdb;Username=postgres;Password=postgres`.

#### Генерация тестовых сообщений

Локальный запуск выполняется через `dotnet run` или IDE (подключение к Kafka через переменные окружения).

В Docker:
```bash
docker compose --profile gen up --build
```

### Конфигурация

#### Переменные окружения

Подключение к Kafka:

- `KAFKA_BOOTSTRAP_SERVERS`, значение для Docker `127.0.0.1:9092`, указано в `launchSettings.json`
- `KAFKA_TOPIC`, значение для Docker `user-events`, указано в `launchSettings.json`
- `KAFKA_GROUP_ID`, значение для Docker `kafka-event-processor-group`, указано в `launchSettings.json`

Подключение к PostgreSQL:
- `POSTGRES_CONNECTION_STRING`, значение для Docker `Host=127.0.0.1;Port=5433;Database=appdb;Username=postgres;Password=postgres`, указано в `launchSettings.json`

#### Файл конфигурации (appsettings.json)

```json
{
  "DataStorage": {
    "BatchSize": 100,   // Размер пакета при записи в БД
    "Type": "JsonFile"  // Тип хранилища, enum DataStorageType (JsonFile, Postgres)
  },
  "EventSaving": {
    "BatchSize": 100,            // Размер пакета событий для сохранения
    "TimerPeriod": "00:01:00",   // Период для сохранения
    "Mode": "BatchOrTimerBased"  // Стратегия сохранения, enum EventSavingMode (BatchBased, TimerBased, BatchOrTimerBased)
  }
}
```
