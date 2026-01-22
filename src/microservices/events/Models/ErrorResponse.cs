using System.ComponentModel.DataAnnotations;

namespace KafkaProducerConsumer.Models
{
    public class ErrorResponse
    {
        [Required]
        public string error { get; set; }
    }
}
