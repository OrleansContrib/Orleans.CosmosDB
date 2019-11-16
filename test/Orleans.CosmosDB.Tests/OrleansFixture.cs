using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.CosmosDB.Tests
{
    public class OrleansFixture : IAsyncLifetime
    {
        public IHost Host { get; }
        public IClusterClient Client { get; }

        private const string ClusterId = "TESTCLUSTER";

        // Use distinct silo ports for each test as they may run in parallel.
        private static int portUniquifier;

        public OrleansFixture()
        {
            string serviceId = Guid.NewGuid().ToString();   // This is required by some tests; Reminders will parse it as a GUID.

            var portInc = Interlocked.Increment(ref portUniquifier);
            var siloPort = EndpointOptions.DEFAULT_SILO_PORT + portInc;
            var gatewayPort = EndpointOptions.DEFAULT_GATEWAY_PORT + portInc;
            var silo = new HostBuilder()
                .ConfigureLogging((context, loggingBuilder) =>
                {
                    loggingBuilder.AddConsole();
                })
                .UseOrleans(b =>
                {
                    b.UseLocalhostClustering();
                    b.ConfigureEndpoints(siloPort, gatewayPort);
                    b.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = ClusterId;
                        options.ServiceId = serviceId;
                    });
                    b.ConfigureApplicationParts(pm => pm.AddApplicationPart(typeof(PersistenceTests).Assembly));
                    PreBuild(b);
                }).Build();

            this.Host = silo;

            this.Client = this.Host.Services.GetRequiredService<IClusterClient>();
        }

        protected virtual ISiloBuilder PreBuild(ISiloBuilder builder) { return builder; }

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

        public Task InitializeAsync() => this.Host.StartAsync();

        public Task DisposeAsync()
        {
            try
            {
                return this.Host.StopAsync();
            }
            catch { return Task.CompletedTask; }
        }
    }
}
