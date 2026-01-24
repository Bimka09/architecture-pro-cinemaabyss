using Confluent.Kafka;
using KafkaProducerConsumer.Models;
using Microsoft.AspNetCore.Mvc;

namespace KafkaProducerConsumer;
public  class HealthRes
{
    public bool status {  get; set; }
}

[ApiController]
[Route("api/events")]
public class EventsController(KafkaProducer producer, IConfiguration configuration) : ControllerBase
{
    private readonly KafkaProducer _producer = producer;

    [HttpGet("health")]
    public async Task<IActionResult> Health()
    {
        return Ok(new HealthRes { status = true });
    }


    [HttpPost("user")]
    public async Task<IActionResult> CreateUserEvent([FromBody] UserEvent userEvent)
    {
        var evt = new EventMessage
        {
            id = Guid.NewGuid().ToString(),
            type = "User",
            payload = userEvent,
            timestamp = new Timestamp(DateTime.UtcNow),
        };

        var deliveryResult = await _producer.ProduceAsync(configuration["USER_TOPIC"]!, evt);
        return CreatedAtAction(nameof(CreateUserEvent), deliveryResult);
    }

    [HttpPost("payment")]
    public async Task<IActionResult> CreatePaymentEvent([FromBody] PaymentEvent paymentEvent)
    {
        var evt = new EventMessage
        {
            id = Guid.NewGuid().ToString(),
            type = "Payment",
            payload = paymentEvent,
            timestamp = new Timestamp(DateTime.UtcNow),
        };

        var deliveryResult = await _producer.ProduceAsync(configuration["PAYMENT_TOPIC"]!, evt);
        return CreatedAtAction(nameof(CreatePaymentEvent), deliveryResult);
    }

    /// <summary>
    /// Создание события фильма
    /// </summary>
    /// <param name="movieEvent"></param>
    /// <returns></returns>
    [HttpPost("movie")]
    public async Task<IActionResult> CreateMovieEvent([FromBody] MovieEvent movieEvent)
    {
        var evt = new EventMessage
        {
            id = Guid.NewGuid().ToString(), 
            type = "Movie",
            payload = movieEvent,
            timestamp = new Timestamp(DateTime.UtcNow),
        };

        var deliveryResult = await _producer.ProduceAsync(configuration["MOVIE_TOPIC"]!, evt);
        return CreatedAtAction(nameof(CreateMovieEvent), deliveryResult);
    }
}
