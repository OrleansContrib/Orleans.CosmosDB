using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence.CosmosDB;
using Orleans.Persistence.CosmosDB.Options;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;
using System;

namespace Orleans.Hosting
{
    public static class StorageExtensions
    {

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorageAsDefault<TPartitionKeyProvider>(this ISiloHostBuilder builder, Action<CosmosDBStorageOptions> configureOptions) where TPartitionKeyProvider : class, IPartitionKeyProvider
        {
            return builder.AddCosmosDBGrainStorage<TPartitionKeyProvider>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorage<TPartitionKeyProvider>(this ISiloHostBuilder builder, string name, Action<CosmosDBStorageOptions> configureOptions) where TPartitionKeyProvider : class, IPartitionKeyProvider
        {
            return builder.ConfigureServices(services =>
            {
                services.TryAddSingleton<IPartitionKeyProvider, TPartitionKeyProvider>();
                services.AddCosmosDBGrainStorage(name, configureOptions);
            });
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorageAsDefault(this ISiloHostBuilder builder, Action<CosmosDBStorageOptions> configureOptions, Type customPartitionKeyProviderType)
        {
            return builder.AddCosmosDBGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions, customPartitionKeyProviderType);
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorage(this ISiloHostBuilder builder, string name, Action<CosmosDBStorageOptions> configureOptions, Type customPartitionKeyProviderType)
        {
            return builder.ConfigureServices(services =>
            {
                if (customPartitionKeyProviderType != null)
                {
                    services.TryAddSingleton(typeof(IPartitionKeyProvider), customPartitionKeyProviderType);
                }
                services.AddCosmosDBGrainStorage(name, configureOptions);
            });
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorageAsDefault(this ISiloHostBuilder builder, Action<CosmosDBStorageOptions> configureOptions)
        {
            return builder.AddCosmosDBGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorage(this ISiloHostBuilder builder, string name, Action<CosmosDBStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddCosmosDBGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorageAsDefault<TPartitionKeyProvider>(this ISiloHostBuilder builder, Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null) where TPartitionKeyProvider : class, IPartitionKeyProvider
        {
            return builder.AddCosmosDBGrainStorage<TPartitionKeyProvider>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorage<TPartitionKeyProvider>(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null) where TPartitionKeyProvider : class, IPartitionKeyProvider
        {
            return builder.ConfigureServices(services =>
            {
                services.TryAddSingleton<IPartitionKeyProvider, TPartitionKeyProvider>();
                services.AddCosmosDBGrainStorage(name, configureOptions);
            });
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorageAsDefault(this ISiloHostBuilder builder, Type customPartitionKeyProviderType, Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null)
        {
            return builder.AddCosmosDBGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, customPartitionKeyProviderType, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage using a custom Partition Key Provider.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorage(this ISiloHostBuilder builder, string name, Type customPartitionKeyProviderType, Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services =>
            {
                if (customPartitionKeyProviderType != null)
                {
                    services.TryAddSingleton(typeof(IPartitionKeyProvider), customPartitionKeyProviderType);
                }
                services.AddCosmosDBGrainStorage(name, configureOptions);
            });
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null)
        {
            return builder.AddCosmosDBGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage.
        /// </summary>
        public static ISiloHostBuilder AddCosmosDBGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddCosmosDBGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCosmosDBGrainStorageAsDefault(this IServiceCollection services, Action<CosmosDBStorageOptions> configureOptions)
        {
            return services.AddCosmosDBGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCosmosDBGrainStorage(this IServiceCollection services, string name, Action<CosmosDBStorageOptions> configureOptions)
        {
            return services.AddCosmosDBGrainStorage(name, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage as the default grain storage.
        /// </summary>
        public static IServiceCollection AddCosmosDBGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null)
        {
            return services.AddCosmosDBGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use Azure CosmosDB storage for grain storage.
        /// </summary>
        public static IServiceCollection AddCosmosDBGrainStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<CosmosDBStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<CosmosDBStorageOptions>(name));
            services.AddTransient<IConfigurationValidator>(sp => new CosmosDBStorageOptionsValidator(sp.GetService<IOptionsSnapshot<CosmosDBStorageOptions>>().Get(name), name));
            services.ConfigureNamedOptionForLogging<CosmosDBStorageOptions>(name);
            services.TryAddSingleton(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.TryAddSingleton<IPartitionKeyProvider, DefaultPartitionKeyProvider>();
            return services.AddSingletonNamedService(name, CosmosDBGrainStorageFactory.Create)
                           .AddSingletonNamedService(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }
    }
}
