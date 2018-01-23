using Newtonsoft.Json;

namespace Orleans.Clustering.CosmosDB.Models
{
    internal class ClusterVersionEntity : BaseEntity
    {
        public override string EntityType => nameof(ClusterVersionEntity);

        [JsonProperty]
        public int ClusterVersion { get; set; } = 0;
    }
}
