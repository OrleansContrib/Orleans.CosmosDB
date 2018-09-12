using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Clustering.CosmosDB
{
    public class CosmosDBClusteringOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_CLUSTER_COLLECTION = "OrleansCluster";
        private const int ORLEANS_CLUSTER_COLLECTION_THROUGHPUT = 400;

        public string AccountEndpoint { get; set; }
        [Redact]
        public string AccountKey { get; set; }
        public bool CanCreateResources { get; set; }
        public string DB { get; set; } = ORLEANS_DB;
        public string Collection { get; set; } = ORLEANS_CLUSTER_COLLECTION;
        public int CollectionThroughput { get; set; } = ORLEANS_CLUSTER_COLLECTION_THROUGHPUT;

        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        [JsonConverter(typeof(StringEnumConverter))]
        public Protocol ConnectionProtocol { get; set; } = Protocol.Tcp;

        /// <summary>
        /// Automatically add/update stored procudures on initialization.  This may result in slight downtime due to stored procedures having to be deleted and recreated in partitioned environments.
        /// Make sure this is false if you wish to strictly control downtime.
        /// </summary>
        public bool AutoUpdateStoredProcedures { get; set; }

        /// <summary>
        /// Delete the database on initialization.  Useful for testing scenarios.
        /// </summary>
        public bool DropDatabaseOnInit { get; set; }

        // TODO: Consistency level for emulator (defaults to Session; https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator)
        internal ConsistencyLevel? GetConsistencyLevel() => this.AccountEndpoint.Contains("localhost") ? (ConsistencyLevel?)ConsistencyLevel.Session : null;
    }
}
