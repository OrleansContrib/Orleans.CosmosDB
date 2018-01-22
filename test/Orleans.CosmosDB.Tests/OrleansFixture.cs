using Orleans.Hosting;
using Orleans.Runtime.Configuration;
using Orleans.Serialization;
using System;
using System.Reflection;

namespace Orleans.CosmosDB.Tests
{
    public class OrleansFixture : IDisposable
    {
        public ISiloHost Silo { get; }
        public IClusterClient Client { get; }

        public OrleansFixture()
        {
            var siloConfig = ClusterConfiguration.LocalhostPrimarySilo();
            siloConfig.Globals.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();
            var silo = new SiloHostBuilder()
                .UseConfiguration(siloConfig)
                .Build();
            silo.StartAsync().Wait();
            this.Silo = silo;

            var clientConfig = ClientConfiguration.LocalhostSilo();

            clientConfig.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();

            var client = new ClientBuilder().UseConfiguration(clientConfig)
                .Build();

            client.Connect().Wait();
            this.Client = client;
        }
        public void Dispose()
        {
            Client.Close().Wait();
            Silo.StopAsync().Wait();
        }
    }
}
