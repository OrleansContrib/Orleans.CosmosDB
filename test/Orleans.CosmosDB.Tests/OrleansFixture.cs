using Orleans.Hosting;
using Orleans.Runtime.Configuration;
using System;

namespace Orleans.CosmosDB.Tests
{
    public class OrleansFixture : IDisposable
    {
        public ISiloHost Silo { get; }
        public IClusterClient Client { get; }

        public OrleansFixture()
        {            
            //siloConfig.Globals.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();
            var builder = new SiloHostBuilder();
            var silo = PreBuild(builder)
                .ConfigureApplicationParts(pm => pm.AddApplicationPart(typeof(PersistenceTests).Assembly))
                .Build();
            silo.StartAsync().Wait();
            this.Silo = silo;
            var clientConfig = ClientConfiguration.LocalhostSilo();

            //clientConfig.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();

            var client = new ClientBuilder()
                .ConfigureApplicationParts(pm => pm.AddApplicationPart(typeof(PersistenceTests).Assembly))
                .UseConfiguration(clientConfig)
                .Build();

            client.Connect().Wait();
            this.Client = client;
        }

        protected virtual ISiloHostBuilder PreBuild(ISiloHostBuilder builder) { return builder; }

        public void Dispose()
        {
            Client.Close().Wait();
            Silo.StopAsync().Wait();
        }
    }
}
