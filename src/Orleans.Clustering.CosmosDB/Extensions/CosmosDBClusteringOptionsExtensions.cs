using System;
using Microsoft.Azure.Cosmos;
using Orleans.Clustering.CosmosDB.Models;

namespace Orleans.Clustering.CosmosDB.Extensions;

internal static class CosmosDBClusteringOptionsExtensions
{
    internal static ThroughputProperties GetThroughputProperties(this CosmosDBClusteringOptions options) =>
        options.ThroughputMode switch
        {
            ThroughputMode.Manual => ThroughputProperties.CreateManualThroughput(options.CollectionThroughput),
            ThroughputMode.Autoscale => ThroughputProperties.CreateAutoscaleThroughput(
                options.CollectionThroughput == 400 ? 4000 : options.CollectionThroughput),
            ThroughputMode.Serverless => null,
            _ => throw new ArgumentOutOfRangeException(nameof(options.ThroughputMode), $"There is no setup for throughput mode {options.ThroughputMode}")
        };
}