using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Clustering.CosmosDB
{
    public class CosmosDBClusteringOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_CLUSTER_COLLECTION = "OrleansCluster";
        private const int ORLEANS_CLUSTER_COLLECTION_THROUGHPUT = 400;

        public CosmosClient Client { get; set; }
        public string AccountEndpoint { get; set; }
        [Redact]
        public string AccountKey { get; set; }
        public bool CanCreateResources { get; set; }
        public string DB { get; set; } = ORLEANS_DB;
        public string Collection { get; set; } = ORLEANS_CLUSTER_COLLECTION;
        public int CollectionThroughput { get; set; } = ORLEANS_CLUSTER_COLLECTION_THROUGHPUT;

        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        /// <summary>
        /// Delete the database on initialization.  Useful for testing scenarios.
        /// </summary>
        public bool DropDatabaseOnInit { get; set; }

        // TODO: Consistency level for emulator (defaults to Session; https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator)
        internal IndexingMode? GetConsistencyLevel() => !string.IsNullOrWhiteSpace(this.AccountEndpoint) && this.AccountEndpoint.Contains("localhost") ? (IndexingMode?)IndexingMode.None : null;
    }
}
