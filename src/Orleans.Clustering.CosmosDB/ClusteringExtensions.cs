using Microsoft.Extensions.DependencyInjection;
using Orleans.Clustering.CosmosDB.Options;
using Orleans.Hosting;
using Orleans.Messaging;
using System;

namespace Orleans.Clustering.CosmosDB
{
    public static class ClusteringExtensions
    {
        public static ISiloHostBuilder UseCosmosDBMembership(this ISiloHostBuilder builder,
            Action<AzureCosmosDBClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBMembership(configureOptions));
        }

        public static ISiloHostBuilder UseCosmosDBMembership(this ISiloHostBuilder builder,
            Action<OptionsBuilder<AzureCosmosDBClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBMembership(configureOptions));
        }

        public static IClientBuilder UseCosmosDBGatewayListProvider(this IClientBuilder builder, Action<AzureCosmosDBGatewayOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBGatewayListProvider(configureOptions));
        }

        public static IClientBuilder UseCosmosDBGatewayListProvider(this IClientBuilder builder, Action<OptionsBuilder<AzureCosmosDBGatewayOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBGatewayListProvider(configureOptions));
        }

        public static IServiceCollection UseCosmosDBMembership(this IServiceCollection services,
            Action<AzureCosmosDBClusteringOptions> configureOptions)
        {
            return services.UseCosmosDBMembership(ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection UseCosmosDBMembership(this IServiceCollection services,
            Action<OptionsBuilder<AzureCosmosDBClusteringOptions>> configureOptions)
        {
            configureOptions?.Invoke(services.AddOptions<AzureCosmosDBClusteringOptions>());
            return services.AddSingleton<IMembershipTable, CosmosDBMembershipTable>();
        }

        public static IServiceCollection UseCosmosDBGatewayListProvider(this IServiceCollection services,
            Action<AzureCosmosDBGatewayOptions> configureOptions)
        {
            return services.UseCosmosDBGatewayListProvider(ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection UseCosmosDBGatewayListProvider(this IServiceCollection services,
            Action<OptionsBuilder<AzureCosmosDBGatewayOptions>> configureOptions)
        {
            configureOptions?.Invoke(services.AddOptions<AzureCosmosDBGatewayOptions>());
            return services.AddSingleton<IGatewayListProvider, CosmosDBGatewayListProvider>();
        }
    }
}
