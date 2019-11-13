using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Messaging;
using System;

namespace Orleans.Hosting
{
    public static class ClusteringExtensions
    {
        public static ISiloHostBuilder UseCosmosDBMembership(this ISiloHostBuilder builder,
           Action<CosmosDBClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBMembership(configureOptions));
        }

        public static ISiloHostBuilder UseCosmosDBMembership(this ISiloHostBuilder builder,
            Action<OptionsBuilder<CosmosDBClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBMembership(configureOptions));
        }

        public static ISiloHostBuilder UseCosmosDBMembership(this ISiloHostBuilder builder)
        {
            return builder.ConfigureServices(services =>
            {
                services.AddOptions<CosmosDBClusteringOptions>();
                services.AddSingleton<IMembershipTable, CosmosDBMembershipTable>();
            });
        }

        public static ISiloBuilder UseCosmosDBMembership(this ISiloBuilder builder,
           Action<CosmosDBClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBMembership(configureOptions));
        }

        public static ISiloBuilder UseCosmosDBMembership(this ISiloBuilder builder,
            Action<OptionsBuilder<CosmosDBClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBMembership(configureOptions));
        }

        public static ISiloBuilder UseCosmosDBMembership(this ISiloBuilder builder)
        {
            return builder.ConfigureServices(services =>
            {
                services.AddOptions<CosmosDBClusteringOptions>();
                services.AddSingleton<IMembershipTable, CosmosDBMembershipTable>();
            });
        }

        public static IClientBuilder UseCosmosDBGatewayListProvider(this IClientBuilder builder, Action<CosmosDBGatewayOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBGatewayListProvider(configureOptions));
        }

        public static IClientBuilder UseCosmosDBGatewayListProvider(this IClientBuilder builder)
        {
            return builder.ConfigureServices(services =>
            {
                services.AddOptions<CosmosDBGatewayOptions>();
                services.AddSingleton<IGatewayListProvider, CosmosDBGatewayListProvider>();
            });
        }

        public static IClientBuilder UseCosmosDBGatewayListProvider(this IClientBuilder builder, Action<OptionsBuilder<CosmosDBGatewayOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCosmosDBGatewayListProvider(configureOptions));
        }

        public static IServiceCollection UseCosmosDBMembership(this IServiceCollection services,
            Action<CosmosDBClusteringOptions> configureOptions)
        {
            return services.UseCosmosDBMembership(ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection UseCosmosDBMembership(this IServiceCollection services,
            Action<OptionsBuilder<CosmosDBClusteringOptions>> configureOptions)
        {
            configureOptions?.Invoke(services.AddOptions<CosmosDBClusteringOptions>());
            return services.AddSingleton<IMembershipTable, CosmosDBMembershipTable>();
        }

        public static IServiceCollection UseCosmosDBGatewayListProvider(this IServiceCollection services,
            Action<CosmosDBGatewayOptions> configureOptions)
        {
            return services.UseCosmosDBGatewayListProvider(ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection UseCosmosDBGatewayListProvider(this IServiceCollection services,
            Action<OptionsBuilder<CosmosDBGatewayOptions>> configureOptions)
        {
            configureOptions?.Invoke(services.AddOptions<CosmosDBGatewayOptions>());
            return services.AddSingleton<IGatewayListProvider, CosmosDBGatewayListProvider>();
        }
    }
}
