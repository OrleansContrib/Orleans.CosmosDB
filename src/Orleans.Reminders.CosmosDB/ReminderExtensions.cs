using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Reminders.CosmosDB;
using System;

namespace Orleans.Hosting
{
    public static class ReminderExtensions
    {
        /// <summary>
        /// Adds reminder storage backed by Azure CosmosDB.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configure">
        /// The delegate used to configure the reminder store.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloHostBuilder"/>, for chaining.
        /// </returns>
        public static ISiloHostBuilder UseCosmosDBReminderService(this ISiloHostBuilder builder, Action<CosmosDBReminderStorageOptions> configure)
        {
            builder.ConfigureServices(services => services.UseCosmosDBReminderService(configure));
            return builder;
        }

        /// <summary>
        /// Adds reminder storage backed by Azure CosmosDB.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="configure">
        /// The delegate used to configure the reminder store.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloBuilder"/>, for chaining.
        /// </returns>
        public static ISiloBuilder UseCosmosDBReminderService(this ISiloBuilder builder, Action<CosmosDBReminderStorageOptions> configure)
        {
            builder.ConfigureServices(services => services.UseCosmosDBReminderService(configure));
            return builder;
        }

        /// <summary>
        /// Adds reminder storage backed by Azure CosmosDB.
        /// </summary>
        /// <param name="services">
        /// The service collection.
        /// </param>
        /// <param name="configure">
        /// The delegate used to configure the reminder store.
        /// </param>
        /// <returns>
        /// The provided <see cref="IServiceCollection"/>, for chaining.
        /// </returns>
        public static IServiceCollection UseCosmosDBReminderService(this IServiceCollection services, Action<CosmosDBReminderStorageOptions> configure)
        {
            services.AddSingleton<IReminderTable, CosmosDBReminderTable>();
            services.Configure(configure);
            services.ConfigureFormatter<CosmosDBReminderStorageOptions>();
            return services;
        }
    }
}
