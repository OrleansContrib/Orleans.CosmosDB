using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Clustering.CosmosDB.Models
{
    internal class SiloEntity : BaseEntity
    {
        public override string EntityType => nameof(SiloEntity);

        [JsonProperty(nameof(Address))]
        public string Address { get; set; }

        [JsonProperty(nameof(Port))]
        public int Port { get; set; }

        [JsonProperty(nameof(Generation))]
        public int Generation { get; set; }

        [JsonProperty(nameof(Hostname))]
        public string Hostname { get; set; }

        [JsonProperty(nameof(Status))]
        [JsonConverter(typeof(StringEnumConverter))]
        public SiloStatus Status { get; set; }

        [JsonProperty(nameof(ProxyPort))]
        public int? ProxyPort { get; set; }

        [JsonProperty(nameof(SiloName))]
        public string SiloName { get; set; }

        [JsonProperty(nameof(SuspectingSilos))]
        public List<string> SuspectingSilos { get; set; } = new List<string>();

        [JsonProperty(nameof(SuspectingTimes))]
        public List<string> SuspectingTimes { get; set; } = new List<string>();

        [JsonProperty(nameof(StartTime))]
        public DateTimeOffset StartTime { get; set; }

        [JsonProperty(nameof(IAmAliveTime))]
        public DateTimeOffset IAmAliveTime { get; set; }

        public static SiloEntity FromDocument(dynamic document)
        {
            var suspectingSilos = document.SuspectingSilos != null ? ((JArray)document.SuspectingSilos).Select(t => t.ToString()).ToList() : null;
            var suspectingTimes = document.SuspectingTimes != null ? ((JArray)document.SuspectingTimes).Select(t => t.ToString()).ToList() : null;
            return new SiloEntity
            {
                Id = document.id,
                ETag = document._etag,
                ClusterId = document.ClusterId,
                Address = document.Address,
                Generation = (int)document.Generation,
                Hostname = document.Hostname,
                IAmAliveTime = (DateTimeOffset)document.IAmAliveTime,
                Port = (int)document.Port,
                ProxyPort = document.ProxyPort ?? null,
                SiloName = document.SiloName,
                StartTime = (DateTimeOffset)document.StartTime,
                Status = (SiloStatus)document.Status,
                SuspectingSilos = suspectingSilos,
                SuspectingTimes = suspectingTimes
            };
        }
    }
}
