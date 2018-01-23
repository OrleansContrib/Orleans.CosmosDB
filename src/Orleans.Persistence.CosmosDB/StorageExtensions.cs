using Orleans.Hosting;
using Orleans.Persistence.CosmosDB;
using Orleans.Persistence.CosmosDB.Options;
using Orleans.Runtime.Configuration;
using System;

namespace Orleans
{
    public static class StorageExtensions
    {
        public static ISiloHostBuilder UseAzureCosmosDBPersistence(this ISiloHostBuilder builder, Action<AzureCosmosDBPersistenceProviderOptions> options)
        {
            return builder.Configure(options);
        }

        public static void AddAzureCosmosDBPersistence(this ClusterConfiguration config, string name)
        {
            config.Globals.RegisterStorageProvider<CosmosDBPersistenceProvider>(name);
        }
    }
}
