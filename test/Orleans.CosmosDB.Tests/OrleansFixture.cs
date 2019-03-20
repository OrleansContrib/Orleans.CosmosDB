using Newtonsoft.Json.Linq;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;

namespace Orleans.CosmosDB.Tests
{
    public class OrleansFixture : IDisposable
    {
        public ISiloHost Silo { get; }
        public IClusterClient Client { get; }

        private const string ClusterId = "TESTCLUSTER";

        // Use distinct silo ports for each test as they may run in parallel.
        private static int portUniquifier;

        public OrleansFixture()
        {
            string serviceId = Guid.NewGuid().ToString();   // This is required by some tests; Reminders will parse it as a GUID.

            //siloConfig.Globals.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();
            var portInc = Interlocked.Increment(ref portUniquifier);
            var siloPort = EndpointOptions.DEFAULT_SILO_PORT + portInc;
            var gatewayPort = EndpointOptions.DEFAULT_GATEWAY_PORT + portInc;
            ClusterConfiguration clusterConfig = ClusterConfiguration.LocalhostPrimarySilo(siloPort, gatewayPort);
            var configDefaults = clusterConfig.Defaults;
            var siloAddress = clusterConfig.PrimaryNode.Address;

            var builder = new SiloHostBuilder();
            var silo = PreBuild(builder)
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = ClusterId;
                    options.ServiceId = serviceId;
                })
                .UseDevelopmentClustering(options => options.PrimarySiloEndpoint = clusterConfig.PrimaryNode)
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
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = ClusterId;
                    options.ServiceId = serviceId;
                })
                .UseStaticClustering(options => options.Gateways = new List<Uri> { new IPEndPoint(siloAddress, gatewayPort).ToGatewayUri() })
                .ConfigureApplicationParts(pm => pm.AddApplicationPart(typeof(PersistenceTests).Assembly))
                .Build();

            client.Connect().Wait();

            this.Client = client;
        }

        protected virtual ISiloHostBuilder PreBuild(ISiloHostBuilder builder) { return builder; }

        public const string TEST_STORAGE = "TEST_STORAGE_PROVIDER";

        internal static void GetAccountInfo(out string accountEndpoint, out string accountKey)
        {
            // Default to emulator
            accountEndpoint = "https://localhost:8081";
            accountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

            if (GetFileInCurrentOrParentDir("CosmosDBTestSecrets.json", out string secretsFile))
            {
                var secrets = JObject.Parse(File.ReadAllText(secretsFile));
                if (secrets.TryGetValue("CosmosDBEndpoint", StringComparison.OrdinalIgnoreCase, out JToken value))
                {
                    accountEndpoint = (string)value;
                }
                if (secrets.TryGetValue("CosmosDBKey", StringComparison.OrdinalIgnoreCase, out value))
                {
                    accountKey = (string)value;
                }
            }
        }

        private static bool GetFileInCurrentOrParentDir(string fileName, out string foundFileName)
        {
            for (var dirName = new DirectoryInfo(Directory.GetCurrentDirectory()); dirName != null && dirName.Exists; dirName = dirName.Parent)
            {
                var filePath = Path.Combine(dirName.FullName, fileName);
                if (File.Exists(filePath))
                {
                    foundFileName = filePath;
                    return true;
                }
            }
            foundFileName = null;
            return false;
        }

        public void Dispose()
        {
            this.Client.Close().Wait();
            this.Silo.StopAsync().Wait();
        }
    }
}
