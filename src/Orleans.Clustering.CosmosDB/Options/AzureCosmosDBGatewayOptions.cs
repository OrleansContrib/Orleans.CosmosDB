using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Clustering.CosmosDB.Options
{
    public class AzureCosmosDBGatewayOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_CLUSTER_COLLECTION = "OrleansCluster";

        [JsonProperty(nameof(ClusterId))]
        public string ClusterId { get; set; }

        [JsonProperty(nameof(AccountEndpoint))]
        public string AccountEndpoint { get; set; }

        [JsonProperty(nameof(AccountKey))]
        public string AccountKey { get; set; }

        [JsonProperty(nameof(DB))]
        public string DB { get; set; } = ORLEANS_DB;

        [JsonProperty(nameof(Collection))]
        public string Collection { get; set; } = ORLEANS_CLUSTER_COLLECTION;

        [JsonProperty(nameof(ConnectionMode))]
        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        [JsonProperty(nameof(ConnectionProtocol))]
        [JsonConverter(typeof(StringEnumConverter))]
        public Protocol ConnectionProtocol { get; set; } = Protocol.Tcp;
    }
}
