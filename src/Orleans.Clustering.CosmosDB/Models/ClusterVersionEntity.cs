using Newtonsoft.Json;

namespace Orleans.Clustering.CosmosDB.Models
{
    internal class ClusterVersionEntity : BaseEntity
    {
        public override string EntityType => nameof(ClusterVersionEntity);

        [JsonProperty]
        public int ClusterVersion { get; set; } = 0;

        public static ClusterVersionEntity FromDocument(dynamic document)
        {
            return new ClusterVersionEntity
            {
                Id = document.id,
                ETag = document._etag,
                ClusterId = document.ClusterId,
                ClusterVersion = document.ClusterVersion
            };
        }
    }
}
