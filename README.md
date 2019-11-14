<p align="center">
  <img src="https://github.com/dotnet/orleans/blob/gh-pages/assets/logo.png" alt="Orleans.CosmosDB" width="300px"> 
  <h1>Orleans CosmosDB Providers</h1>
</p>

[![CI](https://github.com/OrleansContrib/Orleans.CosmosDB/workflows/CI/badge.svg)](https://github.com/OrleansContrib/Orleans.CosmosDB/actions)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Clustering.CosmosDB.svg?style=flat)](http://www.nuget.org/packages/Orleans.Clustering.CosmosDB)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Persistence.CosmosDB.svg?style=flat)](http://www.nuget.org/packages/Orleans.Persistence.CosmosDB)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Reminders.CosmosDB.svg?style=flat)](http://www.nuget.org/packages/Orleans.Reminders.CosmosDB)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Streaming.CosmosDB.svg?style=flat)](http://www.nuget.org/packages/Orleans.Streaming.CosmosDB)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/dotnet/orleans?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

[Orleans](https://github.com/dotnet/orleans) is a framework that provides a straight-forward approach to building distributed high-scale computing applications, without the need to learn and apply complex concurrency or other scaling patterns. 

[Azure CosmosDB](https://azure.microsoft.com/en-us/services/cosmos-db) is Microsoft's globally distributed, multi-model database. It is a database for extremely low latency and massively scalable applications anywhere in the world, with native support for NoSQL. 

**Orleans.CosmosDB** is a package that use Azure CosmosDB as a backend for Orleans providers like Cluster Membership, Grain State storage, Streaming and Reminders. 


# Installation

Installation is performed via [NuGet](https://www.nuget.org/packages?q=Orleans+CosmosDB)

From Package Manager:

> PS> Install-Package Orleans.Clustering.CosmosDB

> PS> Install-Package Orleans.Persistence.CosmosDB

> PS> Install-Package Orleans.Reminders.CosmosDB

> PS> Install-Package Orleans.Streaming.CosmosDB

.Net CLI:

> \# dotnet add package Orleans.Clustering.CosmosDB

> \# dotnet add package Orleans.Persistence.CosmosDB

> \# dotnet add package Orleans.Reminders.CosmosDB

> \# dotnet add package Orleans.Streaming.CosmosDB

Paket: 

> \# paket add Orleans.Clustering.CosmosDB

> \# paket add Orleans.Persistence.CosmosDB

> \# paket add Orleans.Reminders.CosmosDB

> \# paket add Orleans.Streaming.CosmosDB

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

## Custom partition keys for storage provider
By default the grain type is used as partition key, however this limits the storage size for a single grain type as a single logical partition has an upper limit of 10 GB storage.

It is possible to override the default by implementing a custom `IPartitionKeyProvider`. The custom implementation of `IPartitionKeyProvider` can be registered by using the dependency injection usual methods like:

```
services.AddSingleton<IPartitionKeyProvider, MyCustomPKProvider>();
```

There is also some overloads of `AddCosmosDBGrainStorage` and `AddCosmosDBGrainStorageAsDefault` that allow you to pass the `IPartitionKeyProvider` implementation.

If no `IPartitionKeyProvider` is used, the default one will be uses which use full name of the grain type will be used as partition key. 

***Example***
```cs
public class PrimaryKeyPartitionKeyProvider : IPartitionKeyProvider
{
    public ValueTask<string> GetPartitionKey(string grainType, GrainReference grainReference) 
    {
        return new ValueTask<string>(grainReference.GetPrimaryKey().ToString());
    }
}
``` 
The example above use the Primary key as partition key.

In order to prevent cross partition queries the partition key must be available to the client upon reading data, hence the delegate input is limited to the graintype and grainreference. The grain reference contains the grain id, with combination, and a type identifier.

For further details on partitioning in CosmosDB see https://docs.microsoft.com/en-us/azure/cosmos-db/partitioning-overview. 

### Backwards compatibility
The change will not affect existing systems. Configuring a custom `IPartitionKeyProvider` for an existing system will throw a BadGrainStorageConfigException stating incompatibility. To start using a custom partition key provider the old documents must be updated with a `/PartitionKey` property where the value is set using the same functionality as in the `GetPartitionKey` implementation in the custom `IPartitionKeyProvider`. Once all documents are updated, the data must be migrated to a new CosmosDB collection using Azure Data Factory, CosmosDB Migration tool or custom code with colletions PartitionKey set to `PartitionKey`

### Migration from 1.3.0 to 3.0.0

With the 3.0.0 release, two breaking changes requires manual changes if you have pre-existent data:

#### Remove stored procedures

Reminders, Streaming and Persistence providers doesn't rely on Stored Procedures anymore. You can safely remove them from your databases once you migrate to 3.0.0. The only stored procedures that are still used, are the ones from the Clustering packages since Orleans membership protocol requires some atomic operations that are only possible on CosmosDB by using stored procedures. All the rest can be killed.

#### Reminders collection

Before 3.0.0, the reminders provider used to use a non-partitioned collection. Recently Microsoft requires that everyone using CosmosDB to migrate to partitioned collections. If you have data on your old collection, you need to migrate this data to a new one.

The new structure is defined as follow:

- `id` field: `$"{grainRef.ToKeyString()}-{reminderName}"`
- `PartitionKey` field: `$"{serviceId}_{grainRef.GetUniformHashCode():X8}"`
- Other fields remain the same.

This data migration can be performed whatever the way you prefer, as long as the `id` and `PartitionKey` fields are formated the way described here. The partition key path of the new collection must be `/PartitionKey`.

### Indexing
The current indexing fork relies on CosmosDB stored procedures for lookup. As stored procedures must be executed against a specific partition, the use of custom partition key builders is not compatible with the Orleans indexing fork. 

## Stream Provider

To use the Stream Provider you need to register it on your `ISiloBuilder`:

```csharp
.AddCosmosDBStreaming(config => 
    config.AddStream("<provider name>", configure =>
    {
        // The information on FeedCollectionInfo property is related to the database that will be monitored by the change feed
        configure.FeedCollectionInfo = new DocumentCollectionInfo
        {
            Uri = new Uri("<CosmosDB URI>"),
            MasterKey = "<CosmosDB Master Key>" ,
            DatabaseName = "<CosmosDB Database>",
            CollectionName = "<CosmosDB Collection>" 
        };

        // The information on LeaseCollectionInfo is related to the CosmosDB Change Feed lease collection
        configure.LeaseCollectionInfo = new DocumentCollectionInfo
        {
            Uri = new Uri("<CosmosDB Change Feed Lease URI>"),
            MasterKey = "<CosmosDB Change Feed Lease Master Key>" ,
            DatabaseName = "<CosmosDB Change Feed Lease Database>",
            CollectionName = "<CosmosDB Change Feed Lease Collection>" 
        };
    }, typeof(PartitionKeyBasedStreamMapper)))

```

Then on your grain, you need to implement `IAsyncObserver<Document>` in order to receive the document that has changed and published thru Cosmos DB Change Feed.

# Contributions
PRs and feedback are **very** welcome!
