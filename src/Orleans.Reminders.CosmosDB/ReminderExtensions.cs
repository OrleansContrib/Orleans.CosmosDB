using Orleans.Hosting;
using Orleans.Reminders.CosmosDB.Options;
using Orleans.Runtime.Configuration;
using System;

namespace Orleans
{
    public static class ReminderExtensions
    {
        public static ISiloHostBuilder UseAzureCosmosDBReminders(this ISiloHostBuilder builder, Action<AzureCosmosDBReminderProviderOptions> options)
        {
            return builder.Configure(options);
        }

        public static void AddAzureCosmosDBReminders(this ClusterConfiguration config)
        {
            config.Globals.ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.Custom;
            config.Globals.ReminderTableAssembly = "Orleans.Reminders.CosmosDB";
        }
    }
}
