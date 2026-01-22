using Confluent.Kafka;
using System.ComponentModel.DataAnnotations;

namespace KafkaProducerConsumer.Models
{
    public class EventMessage
    {

        [Required]
        public string id { get; set; }
        [Required]
        public string type { get; set; }

        [Required]
        public Timestamp timestamp { get; set; }

        [Required]
        public object payload { get; set; }
    }
}
