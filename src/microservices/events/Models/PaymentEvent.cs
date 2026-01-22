using System.ComponentModel.DataAnnotations;

namespace KafkaProducerConsumer.Models
{
    public class PaymentEvent
    {
        [Required]
        public int user_id { get; set; }

        [Required]
        public int payment_id { get; set; }

        [Required]
        public float amount { get; set; }

        [Required]
        public string status { get; set; }

        [Required]
        public DateTime timestamp { get; set; }

        public string? method_type { get; set; }
    }
}
