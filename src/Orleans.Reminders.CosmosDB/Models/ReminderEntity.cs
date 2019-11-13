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

        [JsonProperty(nameof(PartitionKey))]
        public string PartitionKey { get; set; }

        [JsonProperty(nameof(ServiceId))]
        public string ServiceId { get; set; }

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

        public static string ConstructId(GrainReference grainRef, string reminderName) => $"{grainRef.ToKeyString()}-{reminderName}";

        public static string ConstructPartitionKey(string serviceId, GrainReference grainRef) => ConstructPartitionKey(serviceId, grainRef.GetUniformHashCode());

        public static string ConstructPartitionKey(string serviceId, uint number)
        {
            // IMPORTANT NOTE: Other code using this return data is very sensitive to format changes,
            //       so take great care when making any changes here!!!

            // this format of partition key makes sure that the comparisons in FindReminderEntries(begin, end) work correctly
            // the idea is that when converting to string, negative numbers start with 0, and positive start with 1. Now,
            // when comparisons will be done on strings, this will ensure that positive numbers are always greater than negative
            // string grainHash = number < 0 ? string.Format("0{0}", number.ToString("X")) : string.Format("1{0:d16}", number);

            return $"{serviceId}_{number:X8}";
        }
    }
}
