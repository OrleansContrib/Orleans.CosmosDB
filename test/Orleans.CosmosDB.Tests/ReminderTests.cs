using Orleans.CosmosDB.Tests.Grains;
using Orleans.Hosting;
using System.Net;
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
            private const string DatabaseName = "OrleansRemindersTest";

            protected override ISiloHostBuilder PreBuild(ISiloHostBuilder builder)
            {
                OrleansFixture.GetAccountInfo(out var accountEndpoint, out var accountKey);

                //siloConfig.Globals.ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.Custom;
                //siloConfig.Globals.ReminderTableAssembly = "Orleans.Reminders.CosmosDB";
                //siloConfig.AddAzureCosmosDBPersistence(TEST_STORAGE);
                return builder  //.UseDevelopmentClustering(opt => opt.PrimarySiloEndpoint = new IPEndPoint(IPAddress.Any, 10000))//.UseConfiguration(siloConfig)
                    .AddCosmosDBGrainStorage(OrleansFixture.TEST_STORAGE, opt =>
                    {
                        opt.AccountEndpoint = accountEndpoint;
                        opt.AccountKey = accountKey;
                        opt.ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway;
                        opt.DropDatabaseOnInit = true;
                        opt.CanCreateResources = true;
                        opt.AutoUpdateStoredProcedures = true;
                        opt.InitStage = ServiceLifecycleStage.RuntimeStorageServices;
                        opt.DB = DatabaseName;
                    })
                    .UseCosmosDBReminderService(opt =>
                    {
                        opt.AccountEndpoint = accountEndpoint;
                        opt.AccountKey = accountKey;
                        opt.ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway;
                        opt.CanCreateResources = true;
                        opt.AutoUpdateStoredProcedures = true;
                        opt.DB = DatabaseName;
                    });
            }
        }

        private ReminderFixture _fixture;

        public ReminderTests(ReminderFixture fixture)
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
            await Task.Delay((int)TestGrain.ReminderWaitTime.TotalMilliseconds);
            Assert.True(await grain.ReminderTicked());
            await grain.DismissReminder(test);
            Assert.False(await grain.ReminderExist(test));
        }
    }
}
