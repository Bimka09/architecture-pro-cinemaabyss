using Confluent.Kafka;
using KafkaProducerConsumer.Models;
using System.Text.Json;

namespace KafkaProducerConsumer;

public class KafkaProducer
{
    private readonly IProducer<Null, string> _producer;

    public KafkaProducer(IConfiguration configuration)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = configuration["KAFKA_BROKERS"]
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task<EventResponse> ProduceAsync(string topic, EventMessage message)
    {
        var json = JsonSerializer.Serialize(message);

        var deliveryResult = await _producer.ProduceAsync(
            topic,
            new Message<Null, string> { Value = json });

        Console.WriteLine($"[Producer] Sent to {topic}: {json}");
        return new EventResponse()
        {
            partition = deliveryResult.Partition,
            @event = message,
            status = deliveryResult.Status.ToString().Equals("persisted", StringComparison.InvariantCultureIgnoreCase) ? "success" : "error",
            offset = deliveryResult.Offset.Value,
        };
    }
}
