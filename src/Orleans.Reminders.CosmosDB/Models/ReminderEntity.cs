using Newtonsoft.Json;
using System;

namespace Orleans.Reminders.CosmosDB.Models
{
    internal class ReminderEntity
    {
        private const string ID_FIELD = "id";
        private const string ETAG_FIELD = "_etag";

        [JsonProperty(ID_FIELD)]
        public string Id { get; set; }

        [JsonProperty(nameof(ServiceId))]
        public Guid ServiceId { get; set; }

        [JsonProperty(nameof(GrainId))]
        public string GrainId { get; set; }

        [JsonProperty(nameof(Name))]
        public string Name { get; set; }

        [JsonProperty(nameof(StartAt))]
        public DateTimeOffset StartAt { get; set; }

        [JsonProperty(nameof(Period))]
        public TimeSpan Period { get; set; }

        [JsonProperty(nameof(GrainHash))]
        public uint GrainHash { get; set; }

        [JsonProperty(ETAG_FIELD)]
        public string ETag { get; set; }
    }
}
