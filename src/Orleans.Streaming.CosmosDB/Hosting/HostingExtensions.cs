using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;

namespace Orleans.Streaming.CosmosDB
{
    public static class HostingExtensions
    {
        /// <summary>
        /// Register CosmosDB Change feed stream provider to Orleans runtime.
        /// </summary>
        /// <param name="configure">Configuration builder delegate</param>
        public static ISiloHostBuilder AddCosmosDBStreaming(this ISiloHostBuilder siloBuilder, Action<CosmosDBStreamConfigurator> configure)
        {
            if (configure == null) throw new ArgumentNullException(nameof(configure));

            var configurator = new CosmosDBStreamConfigurator();
            configure.Invoke(configurator);

            foreach (var config in configurator.Settings)
            {
                siloBuilder.AddSimpleMessageStreamProvider(config.Key);
            }

            return siloBuilder
                .Configure(configure)
                .AddGrainService<ChangeFeedService>()
                .ConfigureServices(services => services.AddSingleton<CosmosDBStreamConfigurator>(configurator))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(ChangeFeedService).Assembly).WithReferences());
        }

        /// <summary>
        /// Register CosmosDB Change feed stream provider to Orleans runtime.
        /// </summary>
        /// <param name="configure">Configuration builder delegate</param>
        public static ISiloBuilder AddCosmosDBStreaming(this ISiloBuilder siloBuilder, Action<CosmosDBStreamConfigurator> configure)
        {
            if (configure == null) throw new ArgumentNullException(nameof(configure));

            var configurator = new CosmosDBStreamConfigurator();
            configure.Invoke(configurator);

            foreach (var config in configurator.Settings)
            {
                siloBuilder.AddSimpleMessageStreamProvider(config.Key);
            }

            return siloBuilder
                .Configure(configure)
                .AddGrainService<ChangeFeedService>()
                .ConfigureServices(services => services.AddSingleton<CosmosDBStreamConfigurator>(configurator))
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(ChangeFeedService).Assembly).WithReferences());
        }
    }
}