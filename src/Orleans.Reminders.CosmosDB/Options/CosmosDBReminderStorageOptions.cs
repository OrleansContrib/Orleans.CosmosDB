using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Reminders.CosmosDB
{
    public class CosmosDBReminderStorageOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_REMINDERS_COLLECTION = "OrleansReminders";
        private const int ORLEANS_STORAGE_COLLECTION_THROUGHPUT = 400;
        public CosmosClient Client { get; set; }
        public string AccountEndpoint { get; set; }
        [Redact]
        public string AccountKey { get; set; }
        public string DB { get; set; } = ORLEANS_DB;
        public string Collection { get; set; } = ORLEANS_REMINDERS_COLLECTION;
        public int CollectionThroughput { get; set; } = ORLEANS_STORAGE_COLLECTION_THROUGHPUT;
        public bool CanCreateResources { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        /// <summary>
        /// Delete the database on initialization.  Useful for testing scenarios.
        /// </summary>
        public bool DropDatabaseOnInit { get; set; }

        // TODO: Consistency level for emulator (defaults to Session; https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator)
        internal ConsistencyLevel? GetConsistencyLevel() => !string.IsNullOrWhiteSpace(this.AccountEndpoint) && this.AccountEndpoint.Contains("localhost") ? (ConsistencyLevel?)ConsistencyLevel.Session : null;
    }
}
