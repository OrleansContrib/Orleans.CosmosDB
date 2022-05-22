using Newtonsoft.Json;

namespace Orleans.Persistence.CosmosDB.Models
{
    internal class GrainStateEntity
    {
        private const string ID_FIELD = "id";
        private const string ETAG_FIELD = "_etag";

        [JsonProperty(ID_FIELD)]
        public string Id { get; set; }
        
        [JsonProperty(nameof(GrainType))]
        public string GrainType { get; set; }

        [JsonProperty(nameof(State))]
        public object State { get; set; }

        [JsonProperty(ETAG_FIELD)]
        public string ETag { get; set; }

        [JsonProperty(nameof(PartitionKey))]
        public string PartitionKey { get; set; }
    }
}
