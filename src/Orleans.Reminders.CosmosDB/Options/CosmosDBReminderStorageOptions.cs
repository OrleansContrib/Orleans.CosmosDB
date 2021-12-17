using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Reminders.CosmosDB
{
    public class CosmosDBReminderStorageOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_REMINDERS_COLLECTION = "OrleansReminders";
        private const bool ORLEANS_STORAGE_DEDICATED_THROUGHPUT_ENABLED = true;
        private const bool ORLEANS_STORAGE_SHARED_THROUGHPUT_ENABLED = true;
        private const int ORLEANS_STORAGE_COLLECTION_THROUGHPUT = 400;
        private const bool ORLEANS_STORAGE_AUTOSCALE_THROUGHPUT_ENABLED = false;
        private const int ORLEANS_STORAGE_AUTOSCALE_THROUGHPUT_MAX = 4000;
        public CosmosClient Client { get; set; }
        public string AccountEndpoint { get; set; }
        [Redact]
        public string AccountKey { get; set; }
        public string DB { get; set; } = ORLEANS_DB;
        public int DatabaseThroughput { get; set; } = ORLEANS_STORAGE_COLLECTION_THROUGHPUT;
        public int DatabaseUseSharedThroughput { get; set; } = ORLEANS_STORAGE_SHARED_THROUGHPUT_ENABLED;
        public int DatabaseUseAutoscaleThroughput { get; set; } = ORLEANS_STORAGE_AUTOSCALE_THROUGHPUT_ENABLED;
        public int DatabaseAutoscaleThroughputMax { get; set; } = ORLEANS_STORAGE_AUTOSCALE_THROUGHPUT_MAX;
        public string Collection { get; set; } = ORLEANS_REMINDERS_COLLECTION;
        public int CollectionThroughput { get; set; } = ORLEANS_STORAGE_COLLECTION_THROUGHPUT;
        public bool UseDedicatedThroughput { get; set; } = ORLEANS_STORAGE_DEDICATED_THROUGHPUT_ENABLED;
        public bool UseAutoscaleThroughput { get; set; } = ORLEANS_STORAGE_AUTOSCALE_THROUGHPUT_ENABLED;
        public int AutoscaleThroughputMax { get; set; } = ORLEANS_STORAGE_AUTOSCALE_THROUGHPUT_MAX;
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
