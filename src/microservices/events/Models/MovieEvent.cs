using System.ComponentModel.DataAnnotations;

namespace KafkaProducerConsumer.Models
{
    public class MovieEvent
    {
        [Required]
        public int movie_id { get; set; }

        [Required]
        public string title { get; set; }

        [Required]
        public string action { get; set; }

        public int? user_id { get; set; }
        public float? rating { get; set; }
        public string[]? genres { get; set; }
        public string? description { get; set; }
    }
}
