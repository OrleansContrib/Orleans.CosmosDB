using System;
using Orleans.CosmosDB.Tests.Grains;
using Orleans.Hosting;
using Orleans.Persistence.CosmosDB;
using Orleans.Runtime;
using Orleans.Storage;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static Orleans.CosmosDB.Tests.PersistenceTests;

// For Index coverage CreateDocumentQuery
using Orleans.Persistence.CosmosDB.Options;
using System.Net.Http;
using Microsoft.Azure.Cosmos;
using Orleans.Persistence.CosmosDB.Models;

namespace Orleans.CosmosDB.Tests
{
    public class PrimaryKeyPartitionKeyProvider : IPartitionKeyProvider
    {
        public ValueTask<string> GetPartitionKey(string grainType, GrainReference grainReference)
        {
            if (grainType == typeof(TestCustomPartitionGrain).FullName)
                return new ValueTask<string>(grainReference.GetPrimaryKey().ToString());
            else
                return new ValueTask<string>(grainType);
        }
    }

    public class PersistenceTests : IClassFixture<StorageFixture>
    {
        private const string StorageDbName = "OrleansStorageTest";
        private StorageFixture _fixture;

        public class StorageFixture : OrleansFixture
        {
            internal string AccountEndpoint;
            internal string AccountKey;

            protected override ISiloBuilder PreBuild(ISiloBuilder builder)
            {
                OrleansFixture.GetAccountInfo(out this.AccountEndpoint, out this.AccountKey);

                var httpHandler = new HttpClientHandler()
                {
                    ServerCertificateCustomValidationCallback = (req, cert, chain, errors) => true
                };

                var dbClient = new CosmosClient(
                    this.AccountEndpoint,
                    this.AccountKey,
                    new CosmosClientOptions { ConnectionMode = ConnectionMode.Gateway }
                );
                return builder
                    .AddCosmosDBGrainStorage<PrimaryKeyPartitionKeyProvider>(OrleansFixture.TEST_STORAGE, opt =>
                    {
                        opt.Client = dbClient;
                        opt.DropDatabaseOnInit = true;
                        opt.CanCreateResources = true;
                        opt.DB = StorageDbName;
                    });
            }
        }

        public PersistenceTests(StorageFixture fixture) => this._fixture = fixture;

        private async Task AssertAllTasksCompletedSuccessfullyAsync(IEnumerable<Task> tasks)
        {
            await Task.WhenAll(tasks);
            foreach (var t in tasks)
            {
                Assert.True(t.IsCompletedSuccessfully);
            }
        }

        [Fact]
        public async Task Write_Test()
        {
            var tasks = new List<Task>();
            var ids = new List<GrainReference>();

            for (int i = 0; i < 100; i++)
            {
                var grain = this._fixture.Client.GetGrain<ITestGrain>(i);
                ids.Add(grain as GrainReference);
                tasks.Add(grain.Write($"Test {i}"));
            }

            await AssertAllTasksCompletedSuccessfullyAsync(tasks);
        }

        [Fact]
        public async Task Custom_Partition_Test()
        {
            var guid = Guid.NewGuid();

            var grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Write("Test Partition");
            await grain.Deactivate();

            var storage = this._fixture.Host.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
            var container = storage._container;

            var query = container.GetItemQueryIterator<dynamic>(
                $"SELECT * FROM c WHERE c.PartitionKey = \"" + guid.ToString() + "\"",
                requestOptions: new QueryRequestOptions
                {
                    MaxItemCount = -1,
                    MaxConcurrency = -1,
                    PartitionKey = new PartitionKey(guid.ToString())
                }
            );

            FeedResponse<dynamic> result = await query.ReadNextAsync();
            Assert.Equal(1, result.Count);

            var grain2 = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            var list = await grain2.Read();
            Assert.Single(list);
            Assert.Equal("Test Partition", list[0]);
        }

        [Fact]
        public async Task UpdateDocument()
        {
            var guid = Guid.NewGuid();

            var grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Write("Test Partition");
            await grain.Deactivate();

            await grain.Write("Second test");
            await grain.Deactivate();

            var list = await grain.Read();

            Assert.Equal("Second test", list[1]);
        }

        [Fact]
        public async Task UpdateDocument_FailsOnEtagMismatch()
        {
            var guid = Guid.NewGuid();

            var grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Write("Test Partition");
            await grain.Deactivate();

            //activate grain and read state with etag.
            grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Read();

            //change etag
            var storage = this._fixture.Host.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
            var container = storage._container;

            var query = container.GetItemQueryIterator<GrainStateEntity>(
                $"SELECT * FROM c WHERE c.PartitionKey = \"" + guid.ToString() + "\"",
                requestOptions: new QueryRequestOptions
                {
                    MaxItemCount = -1,
                    MaxConcurrency = -1,
                    PartitionKey = new PartitionKey(guid.ToString())
                }
            );

            FeedResponse<GrainStateEntity> result = await query.ReadNextAsync();
            GrainStateEntity doc = result.Single();
            doc.State = "test";
            var response = await container.ReplaceItemAsync(doc, doc.Id,  new PartitionKey(guid.ToString()));

            await Assert.ThrowsAsync<CosmosConditionNotSatisfiedException>(() => grain.Write("Second test"));
        }

        [Fact]
        public async Task ClearState()
        {
            var guid = Guid.NewGuid();

            var grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Write("Test Partition");
            await grain.ClearState();
            await grain.Deactivate();

            grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);

            var list = await grain.Read();


            Assert.Empty(list);

        }

        [Fact]
        public async Task EtagSetWhenReadingGrainAfterClearingState()
        {
            var guid = Guid.NewGuid();

            var grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Write("Test Partition");
            await grain.ClearState();
            await grain.Deactivate();

            grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Write("Can we write when read state is null");
            await grain.Deactivate();

            grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            var list = await grain.Read();
            Assert.Single(list);
            Assert.Equal("Can we write when read state is null", list.First());
        }

        [Fact]
        public async Task ClearState_BeforeWrite()
        {
            var guid = Guid.NewGuid();

            var grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.ClearState();


        }
    }

}
