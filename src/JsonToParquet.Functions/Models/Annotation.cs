using Newtonsoft.Json;

namespace JsonToParquet.Functions.Models
{
    public class Annotation
    {
        [JsonProperty("sequence_level_annotation")]
        public bool SequenceLevelAnnotation { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("category_id")]
        public long CategoryId { get; set; }

        [JsonProperty("seq_id")]
        public string SeqId { get; set; }

        [JsonProperty("season")]
        public string Season { get; set; }

        [JsonProperty("datetime")]
        public DateTimeOffset Datetime { get; set; }

        [JsonProperty("subject_id")]
        public string SubjectId { get; set; }

        [JsonProperty("count")]
        public object Count { get; set; }

        [JsonProperty("standing")]
        public object Standing { get; set; }

        [JsonProperty("resting")]
        public object Resting { get; set; }

        [JsonProperty("moving")]
        public object Moving { get; set; }

        [JsonProperty("interacting")]
        public object Interacting { get; set; }

        [JsonProperty("young_present")]
        public object YoungPresent { get; set; }

        [JsonProperty("image_id")]
        public string ImageId { get; set; }

        [JsonProperty("location")]
        public string Location { get; set; }
    }
}