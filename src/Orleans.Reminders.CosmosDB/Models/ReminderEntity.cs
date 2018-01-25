using Newtonsoft.Json;
using Orleans.Runtime;
using System;

namespace Orleans.Reminders.CosmosDB.Models
{
    internal class ReminderEntity
    {
        private const string ID_FIELD = "id";
        private const string ETAG_FIELD = "_etag";

        [JsonProperty(ID_FIELD)]
        public string Id { get; set; }

        [JsonProperty(nameof(ClusterId))]
        public string ClusterId { get; set; }

        [JsonProperty(nameof(GrainReference))]
        public GrainReference GrainReference { get; set; }

        [JsonProperty(nameof(Name))]
        public string Name { get; set; }

        [JsonProperty(nameof(StartAt))]
        public DateTimeOffset StartAt { get; set; }

        [JsonProperty(nameof(Period))]
        public TimeSpan Period { get; set; }

        [JsonProperty(nameof(PartitionKey))]
        public ReminderEntityPartitionKey PartitionKey { get; set; }

        [JsonProperty(ETAG_FIELD)]
        public string ETag { get; set; }
    }

    internal class ReminderEntityPartitionKey
    {
        [JsonProperty(nameof(ServiceId))]
        public Guid ServiceId { get; set; }

        [JsonProperty(nameof(GrainRefConsistentHash))]
        public string GrainRefConsistentHash { get; set; }
    }
}
