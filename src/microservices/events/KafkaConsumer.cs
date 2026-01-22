using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class KafkaConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaConsumer> _logger;

    public KafkaConsumer(
        IConfiguration configuration,
        ILogger<KafkaConsumer> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topics = new[]
        {
            _configuration["MOVIE_TOPIC"],
            _configuration["USER_TOPIC"],
            _configuration["PAYMENT_TOPIC"]
        };

        var bootstrapServers = _configuration["KAFKA_BROKERS"];

        // 🔁 основной retry loop
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // 1️⃣ ждём, пока топики появятся
                await WaitForTopicsAsync(
                    topics,
                    bootstrapServers,
                    stoppingToken);

                // 2️⃣ создаём consumer
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = "events-service",
                    ClientId = "events-service-consumer",
                    AutoOffsetReset = AutoOffsetReset.Earliest,

                    EnableAutoCommit = true,
                    SessionTimeoutMs = 10_000,
                    SocketTimeoutMs = 10_000,
                    ReconnectBackoffMs = 5_000,
                    ReconnectBackoffMaxMs = 30_000
                };

                using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                    .SetErrorHandler((_, e) =>
                    {
                        _logger.LogWarning("Kafka error: {Reason}", e.Reason);
                    })
                    .Build();

                consumer.Subscribe(topics);

                _logger.LogInformation(
                    "Kafka consumer started. Topics: {Topics}",
                    string.Join(", ", topics));

                // 3️⃣ consume loop
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);

                        if (result?.Message != null)
                        {
                            _logger.LogInformation(
                                "[Kafka] {Topic}: {Message}",
                                result.Topic,
                                result.Message.Value);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Consume error ({Code})",
                            ex.Error.Code);

                        // Unknown topic / partition → выходим и ждём
                        if (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                            break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Kafka consumer crashed, retrying...");
            }

            // ⏳ пауза перед следующей попыткой
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }

        _logger.LogInformation("Kafka consumer stopped");
    }

    // 🔎 ожидание появления топиков
    private async Task WaitForTopicsAsync(
        IEnumerable<string> topics,
        string bootstrapServers,
        CancellationToken ct)
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        };

        using var admin = new AdminClientBuilder(adminConfig).Build();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));

                var existingTopics = metadata.Topics
                    .Where(t => t.Error.Code == ErrorCode.NoError)
                    .Select(t => t.Topic)
                    .ToHashSet();

                if (topics.All(existingTopics.Contains))
                {
                    _logger.LogInformation("All Kafka topics are available");
                    return;
                }

                _logger.LogInformation(
                    "Waiting for topics: {Topics}",
                    string.Join(", ",
                        topics.Except(existingTopics)));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Kafka metadata not available yet");
            }

            await Task.Delay(3000, ct);
        }
    }
}
