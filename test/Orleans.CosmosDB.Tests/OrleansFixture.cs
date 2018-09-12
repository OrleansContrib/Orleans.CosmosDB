using Newtonsoft.Json.Linq;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Orleans.CosmosDB.Tests
{
    public class OrleansFixture : IDisposable
    {
        public ISiloHost Silo { get; }
        public IClusterClient Client { get; }

        private const string ClusterId = "TESTCLUSTER";
        private static string serviceId = Guid.NewGuid().ToString();   // Reminders will parse this as a GUID.

        public OrleansFixture()
        {
            //siloConfig.Globals.FallbackSerializationProvider = typeof(ILBasedSerializer).GetTypeInfo();
            ClusterConfiguration clusterConfig = ClusterConfiguration.LocalhostPrimarySilo();
            var configDefaults = clusterConfig.Defaults;
            int gatewayPort = configDefaults.ProxyGatewayEndpoint.Port;
            var siloAddress = clusterConfig.PrimaryNode.Address;
            var siloPort = clusterConfig.PrimaryNode.Port;

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
                    options.ClusterId = "TESTCLUSTER";
                    options.ServiceId = "TESTSERVICE";
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
            accountEndpoint = "https://localhost:8081";
            accountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

            if (GetFileInCurrentOrParentDir("CosmosDBSecrets.json", out string secretsFile))
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
            foundFileName = null;
            for (var dirName = new DirectoryInfo(Directory.GetCurrentDirectory()); dirName != null && dirName.Exists; dirName = dirName.Parent)
            {
                var filePath = Path.Combine(dirName.FullName, fileName);
                if (File.Exists(filePath))
                {
                    foundFileName = filePath;
                    break;
                }
            }
            return foundFileName != null;
        }

        public void Dispose()
        {
            this.Client.Close().Wait();
            this.Silo.StopAsync().Wait();
        }
    }
}
