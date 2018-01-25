using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Orleans.Reminders.CosmosDB.Options
{
    public class AzureCosmosDBReminderProviderOptions
    {
        private const string ORLEANS_DB = "Orleans";
        private const string ORLEANS_REMINDERS_COLLECTION = "OrleansReminders";
        private const int ORLEANS_STORAGE_COLLECTION_THROUGHPUT = 400;

        [JsonProperty(nameof(AccountEndpoint))]
        public string AccountEndpoint { get; set; }

        [JsonProperty(nameof(AccountKey))]
        public string AccountKey { get; set; }

        [JsonProperty(nameof(DB))]
        public string DB { get; set; } = ORLEANS_DB;

        [JsonProperty(nameof(Collection))]
        public string Collection { get; set; } = ORLEANS_REMINDERS_COLLECTION;

        [JsonProperty(nameof(CollectionThroughput))]
        public int CollectionThroughput { get; set; } = ORLEANS_STORAGE_COLLECTION_THROUGHPUT;

        [JsonProperty(nameof(CanCreateResources))]
        public bool CanCreateResources { get; set; } = true;

        [JsonProperty(nameof(ConnectionMode))]
        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        [JsonProperty(nameof(ConnectionProtocol))]
        [JsonConverter(typeof(StringEnumConverter))]
        public Protocol ConnectionProtocol { get; set; } = Protocol.Tcp;
    }
}
