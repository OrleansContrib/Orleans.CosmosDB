using System;
using Microsoft.Azure.Cosmos;
using Orleans.Persistence.CosmosDB.Models;

namespace Orleans.Persistence.CosmosDB.Extensions;

internal static class CosmosDBStorageOptionsExtensions
{
    internal static ThroughputProperties GetThroughputProperties(this CosmosDBStorageOptions options) =>
        options.ThroughputMode switch
        {
            ThroughputMode.Manual => ThroughputProperties.CreateManualThroughput(options.CollectionThroughput),
            ThroughputMode.Autoscale => ThroughputProperties.CreateAutoscaleThroughput(
                options.CollectionThroughput == 400 ? 4000 : options.CollectionThroughput),
            ThroughputMode.Serverless => null,
            _ => throw new ArgumentOutOfRangeException(nameof(options.ThroughputMode), $"There is no setup for throughput mode {options.ThroughputMode}")
        };
}