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
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Documents;
using Orleans.Persistence.CosmosDB.Options;
using System.Runtime.Serialization;

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

            protected override ISiloHostBuilder PreBuild(ISiloHostBuilder builder)
            {
                OrleansFixture.GetAccountInfo(out this.AccountEndpoint, out this.AccountKey);

                return builder
                    .AddCosmosDBGrainStorage<PrimaryKeyPartitionKeyProvider>(OrleansFixture.TEST_STORAGE, opt =>
                    {
                        opt.AccountEndpoint = this.AccountEndpoint;
                        opt.AccountKey = this.AccountKey;
                        opt.ConnectionMode = ConnectionMode.Gateway;
                        opt.DropDatabaseOnInit = true;
                        opt.AutoUpdateStoredProcedures = true;
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

            var storage = this._fixture.Silo.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
            IDocumentQuery<dynamic> query = storage._dbClient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(StorageDbName, CosmosDBStorageOptions.ORLEANS_STORAGE_COLLECTION),
                $"SELECT * FROM c WHERE c.PartitionKey = \"" + guid.ToString() + "\"",
                new FeedOptions
                {
                    PopulateQueryMetrics = true,
                    MaxItemCount = -1,
                    MaxDegreeOfParallelism = -1,
                    PartitionKey = new PartitionKey(guid.ToString())
                }).AsDocumentQuery();
            FeedResponse<dynamic> result = await query.ExecuteNextAsync();
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


            grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
            await grain.Write("Second test");
            await grain.Deactivate();

            grain = this._fixture.Client.GetGrain<ITestCustomPartitionGrain>(guid);
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
            var storage = this._fixture.Silo.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
            IDocumentQuery<dynamic> query = storage._dbClient.CreateDocumentQuery(
               UriFactory.CreateDocumentCollectionUri(StorageDbName, CosmosDBStorageOptions.ORLEANS_STORAGE_COLLECTION),
               $"SELECT * FROM c WHERE c.PartitionKey = \"" + guid.ToString() + "\"",
               new FeedOptions
               {
                   PopulateQueryMetrics = true,
                   MaxItemCount = -1,
                   MaxDegreeOfParallelism = -1,
                   PartitionKey = new PartitionKey(guid.ToString())
               }).AsDocumentQuery();
            FeedResponse<dynamic> result = await query.ExecuteNextAsync();
            Document doc = result.Single();
            doc.SetPropertyValue("forceUpdate", "test");
            var response = await storage._dbClient.ReplaceDocumentAsync(doc, new RequestOptions { PartitionKey = new PartitionKey(guid.ToString()) });

            await Assert.ThrowsAsync<SerializationException>(() => grain.Write("Second test"));
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
    }
}
