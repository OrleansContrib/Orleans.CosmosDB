using Orleans.Clustering.CosmosDB;
using Orleans.Clustering.CosmosDB.Extensions;
using Orleans.Clustering.CosmosDB.Models;
using Xunit;

namespace Orleans.CosmosDB.Tests.Extensions;

public class CosmosDBClusteringOptionsExtensionsTests
{
    [Fact]
    public void GetThroughputProperties_ManualMode_SetsThroughputCorrectly()
    {
        //Arrange
        var options = new CosmosDBClusteringOptions
        {
            ThroughputMode = ThroughputMode.Manual
        };

        //Act
        var properties = options.GetThroughputProperties();

        //Assert
        Assert.Equal(400, properties.Throughput);
        Assert.Null(properties.AutoscaleMaxThroughput);
    }

    [Fact]
    public void GetThroughputProperties_AutoscaleModeDefaultThroughput_CreatesAutoscaleThroughputWith4000RUs()
    {
        //Arrange
        var options = new CosmosDBClusteringOptions
        {
            ThroughputMode = ThroughputMode.Autoscale
        };

        //Act
        var properties = options.GetThroughputProperties();

        //Assert
        Assert.Equal(4000, properties.AutoscaleMaxThroughput);
        Assert.Null(properties.Throughput);
    }

    [Fact]
    public void GetThroughputProperties_AutoscaleModeCustomThroughput_CreatesAutoscaleThroughputWithCustomRUs()
    {
        //Arrange
        var options = new CosmosDBClusteringOptions
        {
            ThroughputMode = ThroughputMode.Autoscale,
            CollectionThroughput = 6000
        };

        //Act
        var properties = options.GetThroughputProperties();

        //Assert
        Assert.Equal(6000, properties.AutoscaleMaxThroughput);
        Assert.Null(properties.Throughput);
    }

    [Fact]
    public void GetThroughputProperties_ServerlessMode_ReturnsNullThroughput()
    {
        //Arrange
        var options = new CosmosDBClusteringOptions
        {
            ThroughputMode = ThroughputMode.Serverless
        };

        //Act
        var properties = options.GetThroughputProperties();

        //Assert
        Assert.Null(properties);
    }
}