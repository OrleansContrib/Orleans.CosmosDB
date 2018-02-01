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
We need to configure the Orleans Silo with the below:
* Use `.UseCosmosDBMembership()` on `ISiloHostBuilder` to enable cluster membership.
* Use `.AddAzureCosmosDBPersistence()` on `ClusterConfiguration` to register the grain state persistence provider.
* Use `.UseAzureCosmosDBPersistence()` on `ISiloHostBuilder` to configure the grain state persistence provider.
* Use `.AddAzureCosmosDBReminders()` on `ClusterConfiguration` to register the reminders provider.
* Use `.UseAzureCosmosDBReminders()` on `ISiloHostBuilder` to configure the reminders provider.

***Example***
```cs
var siloConfig = new ClusterConfiguration();
siloConfig.AddAzureCosmosDBPersistence("myProvider"); //Register the persistence provider.
siloConfig.AddAzureCosmosDBReminders(); // Register the reminders provider.

var silo = new SiloHostBuilder()
    .UseConfiguration(siloConfig)
    .UseCosmosDBMembership() // Use CosmosDB as the cluster membership table.
    .UseAzureCosmosDBPersistence() // Configure CosmosDB persistence provider.
    .UseAzureCosmosDBReminders() // Configure CosmosDB reminders provider.
    .Build();
await silo.StartAsync();
```

## Client
Now your client application needs to connect to the Orleans Cluster by using configuring the Gateway List Provider:
* Use `.UseCosmosDBGatewayListProvider()` on `IClientBuilder`.

***Example***
```cs
var clientConfig = new ClientConfiguration();

var client = new ClientBuilder()
    .UseConfiguration(clientConfig)
    .UseCosmosDBGatewayListProvider() // Use CosmosDB as the Gateway list provider.
    .Build();
    await client.Connect();
```


Great! Now you have Orleans configured to use Azure CosmosDB as the back storage for its providers!

# Contributions
PRs and feedback are **very** welcome!