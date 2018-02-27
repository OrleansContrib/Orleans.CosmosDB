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
