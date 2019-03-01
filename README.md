<p align="center">
  <img src="https://github.com/dotnet/orleans/blob/gh-pages/assets/logo.png" alt="Orleans.CosmosDB" width="300px"> 
  <h1>Orleans CosmosDB Providers</h1>
</p>

[![CircleCI](https://circleci.com/gh/OrleansContrib/Orleans.CosmosDB.svg?style=svg)](https://circleci.com/gh/OrleansContrib/Orleans.CosmosDB)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Clustering.CosmosDB.svg?style=flat)](http://www.nuget.org/packages/Orleans.Clustering.CosmosDB)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Persistence.CosmosDB.svg?style=flat)](http://www.nuget.org/packages/Orleans.Persistence.CosmosDB)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Reminders.CosmosDB.svg?style=flat)](http://www.nuget.org/packages/Orleans.Clustering.CosmosDB)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/dotnet/orleans?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

[Orleans](https://github.com/dotnet/orleans) is a framework that provides a straight-forward approach to building distributed high-scale computing applications, without the need to learn and apply complex concurrency or other scaling patterns. 

[Azure CosmosDB](https://azure.microsoft.com/en-us/services/cosmos-db) is Microsoft's globally distributed, multi-model database. It is a database for extremely low latency and massively scalable applications anywhere in the world, with native support for NoSQL. 

**Orleans.CosmosDB** is a package that use Azure CosmosDB as a backend for Orleans providers like Cluster Membership, Grain State storage, Streaming and Reminders. 


# Installation

Installation is performed via [NuGet](https://www.nuget.org/packages?q=Orleans+CosmosDB)

From Package Manager:

> PS> Install-Package Orleans.Clustering.CosmosDB -prerelease

> PS> Install-Package Orleans.Persistence.CosmosDB -prerelease

> PS> Install-Package Orleans.Reminders.CosmosDB -prerelease

.Net CLI:

> \# dotnet add package Orleans.Clustering.CosmosDB -prerelease

> \# dotnet add package Orleans.Persistence.CosmosDB -prerelease

> \# dotnet add package Orleans.Reminders.CosmosDB -prerelease

Paket: 

> \# paket add Orleans.Clustering.CosmosDB -prerelease

> \# paket add Orleans.Persistence.CosmosDB -prerelease

> \# paket add Orleans.Reminders.CosmosDB -prerelease

# Configuration

It is not mandatory to use all the providers at once. Just pick the one you are interested in from the samples and you should be good as they don't depend on each other.

## Silo

***Example***
```cs

var silo = new SiloHostBuilder()
    .UseCosmosDBMembership(opt => 
    {
        // Use CosmosDB as the cluster membership table.
        // Configure CosmosDB settings in opt.
    }) 
    .AddCosmosDBGrainStorageAsDefault(opt => 
    {
        // Configure CosmosDB persistence provider.
        // Configure CosmosDB settings in opt.
    }) 
    .UseCosmosDBReminderService(op => 
    {
        // Configure CosmosDB reminders provider.
        // Configure CosmosDB settings in opt.
    }) 
    .Build();
await silo.StartAsync();
```

## Client

***Example***
```cs
var clientConfig = new ClientConfiguration();

var client = new ClientBuilder()
    .UseCosmosDBGatewayListProvider(opt => 
    {
        // Use CosmosDB as the Gateway list provider.
        // Configure CosmosDB settings in opt.
    }) 
    .Build();
    await client.Connect();
```


Great! Now you have Orleans configured to use Azure CosmosDB as the back storage for its providers!

## Custom partition keys
By default the grain type is used as partition key, however this limits the storage size for a single grain type as a single logical partition has an upper limit of 10 GB storage.

It is possible to override the default partition key per grain type by congfiguring delegates on the CosmosDBStorageOptions. In order to prevet cross partition queries the partition key must be available to the client upon reading data, hence the delegate input is limited to the grainreference. The grain reference contains the grain id, with combination, and a type identifier.


***Example***
```cs
opt.AddPartitionKeyBuilder<SomeGrain>(reference
                            => BitConverter.ToInt32(reference.GetPrimaryKey().ToByteArray(), 2).ToString());
``` 
The example above use parts of the primary key for a grain which implements IGrainWithGuidKey.

For further details on partitioning in CosmosDB see https://docs.microsoft.com/en-us/azure/cosmos-db/partitioning-overview. 

***Do note that indexing with custom partition keys is not supported***

# Contributions
PRs and feedback are **very** welcome!