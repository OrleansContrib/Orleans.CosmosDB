using Microsoft.Azure.Documents.Client;
using Orleans.CosmosDB.Tests.Grains;
using Orleans.Hosting;
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;
using static Orleans.CosmosDB.Tests.ReminderTests;

namespace Orleans.CosmosDB.Tests
{
    public class ReminderTests : IClassFixture<ReminderFixture>
    {
        public class ReminderFixture : OrleansFixture
        {
            private const string DatabaseName = "OrleansRemindersTest";

            protected override ISiloBuilder PreBuild(ISiloBuilder builder)
            {
                OrleansFixture.GetAccountInfo(out var accountEndpoint, out var accountKey);

                var httpHandler = new HttpClientHandler()
                {
                    ServerCertificateCustomValidationCallback = (req, cert, chain, errors) => true
                };

                var dbClient = new DocumentClient(new Uri(accountEndpoint), accountKey, httpHandler, new ConnectionPolicy { ConnectionMode = ConnectionMode.Gateway, ConnectionProtocol = Protocol.Https });

                return builder
                    .AddMemoryGrainStorage(OrleansFixture.TEST_STORAGE)
                    .UseCosmosDBReminderService(opt =>
                    {
                        opt.Client = dbClient;
                        opt.CanCreateResources = true;
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
            var test = "grain1";
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
