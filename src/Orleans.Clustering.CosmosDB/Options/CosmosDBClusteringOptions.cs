using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Orleans.Clustering.CosmosDB.Models;

namespace Orleans.Clustering.CosmosDB
{
    public class CosmosDBClusteringOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_CLUSTER_COLLECTION = "OrleansCluster";
        private const int ORLEANS_CLUSTER_COLLECTION_THROUGHPUT = 400;

        /// <summary>
        /// The <see cref="CosmosClient"/> used for clustering.
        /// </summary>
        public CosmosClient Client { get; set; }

        /// <summary>
        /// Gets or sets the cosmos account endpoint URI. This can be retrieved from the Overview section of the Azure Portal.
        /// This is required if you are authenticating using tokens.
        /// <remarks>
        /// In the form of https://{databaseaccount}.documents.azure.com:443/, see: https://docs.microsoft.com/en-us/rest/api/cosmos-db/cosmosdb-resource-uri-syntax-for-rest
        /// </remarks>
        /// </summary>
        public string AccountEndpoint { get; set; }

        /// <summary>
        /// The account key used for a cosmos DB account.
        /// </summary>
        [Redact]
        public string AccountKey { get; set; }

        /// <summary>
        /// Tries to create the database and container used for clustering if it does not exist.
        /// </summary>
        public bool CanCreateResources { get; set; }

        /// <summary>
        /// The name of the database to use for clustering information.
        /// </summary>
        public string DB { get; set; } = ORLEANS_DB;

        /// <summary>
        /// The name of the collection/container to use to store clustering information.
        /// </summary>
        public string Collection { get; set; } = ORLEANS_CLUSTER_COLLECTION;

        /// <summary>
        /// The RU throughput used for the collection/container storing clustering information.
        /// </summary>
        public int CollectionThroughput { get; set; } = ORLEANS_CLUSTER_COLLECTION_THROUGHPUT;

        /// <summary>
        /// The connection mode to use when connecting to the azure cosmos DB service.
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        /// <summary>
        /// Delete the database on initialization.  Useful for testing scenarios.
        /// </summary>
        public bool DropDatabaseOnInit { get; set; }

        /// <summary>
        /// The throughput mode to use for the collection/container used for clustering.
        /// </summary>
        /// <remarks>If the throughput mode is set to Autoscale then the <see cref="CollectionThroughput"/> will need to be at least 4000 RUs.</remarks>
        public ThroughputMode ThroughputMode { get; set; } = ThroughputMode.Manual;

        // TODO: Consistency level for emulator (defaults to Session; https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator)
        internal IndexingMode? GetConsistencyLevel() => !string.IsNullOrWhiteSpace(this.AccountEndpoint) && this.AccountEndpoint.Contains("localhost") ? (IndexingMode?)IndexingMode.None : null;
    }
}
