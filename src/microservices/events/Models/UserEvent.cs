using System.ComponentModel.DataAnnotations;

namespace KafkaProducerConsumer.Models
{
    public class UserEvent
    {
        [Required]
        public int user_id { get; set; }
        public string? username { get; set; }
        public string? email { get; set; }

        [Required]
        public string action { get; set; }

        [Required]
        public DateTime timestamp { get; set; }
    }

}
