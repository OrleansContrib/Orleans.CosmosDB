using Newtonsoft.Json;
using System;

namespace Orleans.Clustering.CosmosDB.Models
{
    internal abstract class BaseEntity
    {
        private const string ID_FIELD = "id";
        private const string ETAG_FIELD = "_etag";

        [JsonProperty(nameof(EntityType))]
        public abstract string EntityType { get; }

        [JsonProperty(ID_FIELD)]
        public string Id { get; set; }

        [JsonProperty(ETAG_FIELD)]
        public string ETag { get; set; }
    }
}
