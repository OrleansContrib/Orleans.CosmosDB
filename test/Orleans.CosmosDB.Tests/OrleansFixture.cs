using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Net;
using System.Threading.Tasks;

namespace Orleans.CosmosDB.Tests
{
    public class OrleansFixture : IDisposable
    {
        public ISiloHost Silo { get; }
        public IClusterClient Client { get; }

        public OrleansFixture()
        {
            var siloPort = 11111;
            int gatewayPort = 30000;
            var siloAddress = IPAddress.Loopback;

            //siloConfig.Globals.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();
            ClusterConfiguration clusterConfig = ClusterConfiguration.LocalhostPrimarySilo();
            var builder = new SiloHostBuilder();
            var silo = PreBuild(builder)
                .Configure(options => options.ClusterId = "TESTCLUSTER")
                .UseDevelopmentClustering(options => options.PrimarySiloEndpoint = new IPEndPoint(siloAddress, siloPort))
                .ConfigureEndpoints(siloAddress, siloPort, gatewayPort)
                //.UseConfiguration(clusterConfig)
                .ConfigureApplicationParts(pm => pm.AddApplicationPart(typeof(PersistenceTests).Assembly))
                .Build();
            silo.StartAsync().Wait();
            this.Silo = silo;

            //clientConfig.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();

            ClientConfiguration clientConfig = ClientConfiguration.LocalhostSilo();
            var client = new ClientBuilder()
                //.UseConfiguration(clientConfig)
                .ConfigureCluster(c => c.ClusterId = "TESTCLUSTER")
                .UseStaticClustering(options => options.Gateways = new[] { new IPEndPoint(siloAddress, gatewayPort).ToGatewayUri() })
                .ConfigureApplicationParts(pm => pm.AddApplicationPart(typeof(PersistenceTests).Assembly))
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
