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
The change will not affect existing systems. Configuring custom partition key builders for an existing system will throw a BadGrainStorageConfigException stating incompatibility. To start using custom partition key builders the old documents must be updated with a /PartitionKey property where the value is set using the same functionality as the Func<GrainReference, string> delegate specified in options. Once all documents are updated, the data must be migrated to a new CosmosDB collection using Azure Data Factory, CosmosDB Migration tool or custom code. 

### Indexing
The current indexing fork relies on CosmosDB stored procedures for lookup. As stored procedures must be executed against a specific partition, the use of custom partition key builders is not compatible with the Orleans indexing fork. 

# Contributions
PRs and feedback are **very** welcome!