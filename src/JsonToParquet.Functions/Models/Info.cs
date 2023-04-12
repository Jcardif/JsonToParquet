using Newtonsoft.Json;

namespace JsonToParquet.Functions.Models
{
    public class Info
    {
        [JsonProperty("version")]
        public string Version { get; set; }

        [JsonProperty("description")]
        public string Description { get; set; }

        [JsonProperty("date_created")]
        public long DateCreated { get; set; }

        [JsonProperty("contributor")]
        public string Contributor { get; set; }
    }
}