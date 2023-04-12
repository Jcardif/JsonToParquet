using Newtonsoft.Json;

namespace JsonToParquet.Functions.Models
{
    public class Image
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("file_name")]
        public string FileName { get; set; }

        [JsonProperty("frame_num")]
        public long FrameNum { get; set; }

        [JsonProperty("seq_id")]
        public string SeqId { get; set; }

        [JsonProperty("width")]
        public long Width { get; set; }

        [JsonProperty("height")]
        public long Height { get; set; }

        [JsonProperty("corrupt")]
        public bool Corrupt { get; set; }

        [JsonProperty("location")]
        public string Location { get; set; }

        [JsonProperty("seq_num_frames")]
        public long SeqNumFrames { get; set; }

        [JsonProperty("datetime")]
        public DateTimeOffset Datetime { get; set; }
    }
}