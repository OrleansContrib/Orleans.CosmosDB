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