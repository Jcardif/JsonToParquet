using Newtonsoft.Json;

namespace JsonToParquet.Functions.Models
{
    public class SerengetiData
    {
        [JsonProperty("info")]
        public Info Info { get; set; }

        [JsonProperty("categories")]
        public List<Category> Categories { get; set; }

        [JsonProperty("images")]
        public List<Image> Images { get; set; }

        [JsonProperty("annotations")]
        public List<Annotation> Annotations { get; set; }

        public SerengetiData()
        {
            Info = new Info();
            Categories = new List<Category>();
            Images = new List<Image>();
            Annotations = new List<Annotation>();
        }
    }
}