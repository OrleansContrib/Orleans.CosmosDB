using Orleans.CosmosDB.Tests.Grains;
using Orleans.Hosting;
using Orleans.Runtime.Configuration;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static Orleans.CosmosDB.Tests.ReminderTests;

namespace Orleans.CosmosDB.Tests
{
    public class ReminderTests : IClassFixture<ReminderFixture>
    {
        public class ReminderFixture : OrleansFixture
        {
            private const string ACC_ENDPOINT = "https://localhost:8081";
            private const string ACC_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            public const string TEST_STORAGE = "TEST_STORAGE_PROVIDER";
            protected override ISiloHostBuilder PreBuild(ISiloHostBuilder builder)
            {
                var siloConfig = ClusterConfiguration.LocalhostPrimarySilo();
                siloConfig.Globals.ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.Custom;
                siloConfig.Globals.ReminderTableAssembly = "Orleans.Reminders.CosmosDB";
                siloConfig.AddAzureCosmosDBReminders();
                siloConfig.AddAzureCosmosDBPersistence(TEST_STORAGE);
                return builder.UseConfiguration(siloConfig)
                    .UseAzureCosmosDBPersistence(opt =>
                    {
                        opt.AccountEndpoint = ACC_ENDPOINT;
                        opt.AccountKey = ACC_KEY;
                        opt.ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway;
                        opt.ConnectionProtocol = Microsoft.Azure.Documents.Client.Protocol.Https;
                        opt.AutoUpdateStoredProcedures = true;
                    })
                    .UseAzureCosmosDBReminders(opt =>
                    {
                        opt.AccountEndpoint = ACC_ENDPOINT;
                        opt.AccountKey = ACC_KEY;
                        opt.ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway;
                        opt.ConnectionProtocol = Microsoft.Azure.Documents.Client.Protocol.Https;
                        opt.DropDatabaseOnInit = true;
                        opt.AutoUpdateStoredProcedures = true;
                    });
            }
        }

        private ReminderFixture _fixture;

        public ReminderTests(ITestOutputHelper log, ReminderFixture fixture)
        {
            this._fixture = fixture;
        }

        [Fact]
        public async Task CreateReminderTest()
        {
            var grain = _fixture.Client.GetGrain<ITestGrain>(0);
            var test = "grain";
            var reminder = await grain.RegisterReminder(test);
            Assert.NotNull(reminder);
            Assert.True(await grain.ReminderExist(test));
            await Task.Delay(60 * 1000);
            Assert.True(await grain.ReminderTicked());
            await grain.DismissReminder(test);
            Assert.False(await grain.ReminderExist(test));
        }
    }
}
