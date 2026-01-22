using System.ComponentModel.DataAnnotations;

namespace KafkaProducerConsumer.Models
{
    public class EventResponse
    {

        [Required]
        public int partition { get; set; }
        [Required]
        public string status { get; set; }

        [Required]
        public long offset { get; set; }

        [Required]
        public EventMessage @event { get; set; }
    }
}
