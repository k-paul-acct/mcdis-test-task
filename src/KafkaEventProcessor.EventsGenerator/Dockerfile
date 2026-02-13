FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app

COPY . .
RUN dotnet restore KafkaEventProcessor.EventsGenerator/KafkaEventProcessor.EventsGenerator.csproj
RUN dotnet publish KafkaEventProcessor.EventsGenerator/KafkaEventProcessor.EventsGenerator.csproj -o out

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app/out .
ENTRYPOINT ["dotnet", "KafkaEventProcessor.EventsGenerator.dll"]
