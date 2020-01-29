using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Clustering.CosmosDB
{
    public class CosmosDBGatewayOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_CLUSTER_COLLECTION = "OrleansCluster";

        public CosmosClient Client { get; set; }
        public string AccountEndpoint { get; set; }
        [Redact]
        public string AccountKey { get; set; }
        public string DB { get; set; } = ORLEANS_DB;
        public string Collection { get; set; } = ORLEANS_CLUSTER_COLLECTION;

        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;
    }
}
