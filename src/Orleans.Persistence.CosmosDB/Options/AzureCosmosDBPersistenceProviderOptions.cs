using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Collections.Generic;

namespace Orleans.Persistence.CosmosDB.Options
{
    public class AzureCosmosDBPersistenceProviderOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_STORAGE_COLLECTION = "OrleansStorage";
        private const int ORLEANS_STORAGE_COLLECTION_THROUGHPUT = 400;
        
        [JsonProperty(nameof(AccountEndpoint))]
        public string AccountEndpoint { get; set; }

        [JsonProperty(nameof(AccountKey))]
        public string AccountKey { get; set; }

        [JsonProperty(nameof(DB))]
        public string DB { get; set; } = ORLEANS_DB;

        [JsonProperty(nameof(Collection))]
        public string Collection { get; set; } = ORLEANS_STORAGE_COLLECTION;

        [JsonProperty(nameof(CollectionThroughput))]
        public int CollectionThroughput { get; set; } = ORLEANS_STORAGE_COLLECTION_THROUGHPUT;

        [JsonProperty(nameof(CanCreateResources))]
        public bool CanCreateResources { get; set; }

        [JsonProperty(nameof(ConnectionMode))]
        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        [JsonProperty(nameof(ConnectionProtocol))]
        [JsonConverter(typeof(StringEnumConverter))]
        public Protocol ConnectionProtocol { get; set; } = Protocol.Tcp;

        [JsonProperty(nameof(DeleteOnClear))]
        public bool DeleteOnClear { get; set; }

        /// <summary>
        /// List of JSON path strings.
        /// Each entry on this list represents a property in the State Object that will be included in the document index.
        /// The default is to not add any property in the State object.
        /// </summary>
        [JsonProperty(nameof(StateFieldsToIndex))]
        public List<string> StateFieldsToIndex { get; set; } = new List<string>();

        /// <summary>
        /// Automatically add/update stored procudures on initialization.  This may result in slight downtime due to stored procedures having to be deleted and recreated in partitioned environments.
        /// Make sure this is false if you wish to strictly control downtime.
        /// </summary>
        [JsonProperty(nameof(AutoUpdateStoredProcedures))]
        public bool AutoUpdateStoredProcedures { get; set; }

        /// <summary>
        /// Delete the database on initialization.  Useful for testing scenarios.
        /// </summary>
        [JsonProperty(nameof(DropDatabaseOnInit))]
        public bool DropDatabaseOnInit { get; set; }
    }
}
