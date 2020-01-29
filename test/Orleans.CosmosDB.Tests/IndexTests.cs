using Orleans.CosmosDB.Tests.Grains;
using Orleans.Hosting;
using Orleans.Persistence.CosmosDB;
using Orleans.Runtime;
using Orleans.Storage;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

// For Index coverage CreateDocumentQuery
using static Orleans.CosmosDB.Tests.IndexTests;
using System.Net.Http;
using Microsoft.Azure.Cosmos;

namespace Orleans.CosmosDB.Tests
{


    public class IndexTests : IClassFixture<StorageIndexFixture>
    {
        private const string StorageDbName = "OrleansStorageTest";
        private StorageIndexFixture _fixture;

        public class StorageIndexFixture : OrleansFixture
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
                    .AddCosmosDBGrainStorage(OrleansFixture.TEST_STORAGE, opt =>
                    {
                        opt.Client = dbClient;
                        opt.DropDatabaseOnInit = true;
                        opt.CanCreateResources = true;
                        opt.DB = StorageDbName;
                        opt.StateFieldsToIndex.Add("NftIndexedInt");
                        opt.StateFieldsToIndex.Add("UserState.FtIndexedString");
                    });
            }
        }

        public IndexTests(StorageIndexFixture fixture) => this._fixture = fixture;

        private async Task AssertAllTasksCompletedSuccessfullyAsync(IEnumerable<Task> tasks)
        {
            await Task.WhenAll(tasks);
            foreach (var t in tasks)
            {
                Assert.True(t.IsCompletedSuccessfully);
            }
        }

        [Fact]
        public async Task Index_Test()
        {
            var tasks = new List<Task>();

            const int mod = 10;
            const int max = 100;
            int nftValue(int value) => value % mod;
            string ftValue(int value) => $"FtIndex {value}";
            string nonValue(int value) => $"NonIndex {value}";
            int parseIntValue(string data) => int.Parse(data.Substring(data.LastIndexOf(" ") + 1));

            for (int i = 0; i < max; i++)
            {
                var grain = this._fixture.Client.GetGrain<ITestIndexedPropertiesGrain>(i) as ITestIndexedPropertiesGrain;

                // NftIndexedInt has multiple entities per key value.
                await grain.SetNftIndexedIntAsync(nftValue(i));

                // FtIndexedString has a single entity per key value (as does NonIndexedString).
                await grain.SetFtIndexedStringAsync(ftValue(i));
                await grain.SetNonIndexedStringAsync(nonValue(i));
                tasks.Add(grain.WriteAsync());
            }

            await this.AssertAllTasksCompletedSuccessfullyAsync(tasks);

            var storage = this._fixture.Host.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
            string grainTypeName() => typeof(TestIndexedPropertiesGrain).FullName;

            // Use the Client GrainReferenceConverter here to obtain a grain in OutsideClientRuntime.
            var grainReferenceConverter = (IGrainReferenceConverter)this._fixture.Client.ServiceProvider.GetService(typeof(IGrainReferenceConverter));
            ITestIndexedPropertiesGrain castToClientSpace(GrainReference grainRef)
                => grainReferenceConverter.GetGrainFromKeyString(grainRef.ToKeyString()).Cast<ITestIndexedPropertiesGrain>() as ITestIndexedPropertiesGrain;

            // One entity per key value.
            for (int i = 0; i < max; i++)
            {
                var grains = await storage.LookupAsync(grainTypeName(), "UserState.FtIndexedString", ftValue(i));
                Assert.Single(grains);
                var grain = castToClientSpace(grains[0]);
                Assert.Equal(nftValue(i), await grain.GetNftIndexedIntAsync());
                Assert.Equal(ftValue(i), await grain.GetFtIndexedStringAsync());
                Assert.Equal(nonValue(i), await grain.GetNonIndexedStringAsync());
            }

            // Multiple entities per key value.
            for (int i = 0; i < mod; i++)
            {
                var grains = await storage.LookupAsync(grainTypeName(), "NftIndexedInt", nftValue(i));
                Assert.Equal(max / mod, grains.Count);
                foreach (var grain in grains.Select(g => castToClientSpace(g)))
                {
                    Assert.Equal(i, await grain.GetNftIndexedIntAsync());
                    Assert.True(parseIntValue(await grain.GetFtIndexedStringAsync()) % mod == i);
                    Assert.True(parseIntValue(await grain.GetNonIndexedStringAsync()) % mod == i);
                }
            }

            // TODO: CHECK HOW TO GET METRICS ON THE NEW SDK
            //var container = storage._container;

            //var query = container.GetItemQueryIterator<dynamic>(
            //   $"SELECT * FROM c WHERE c.GrainType = \"{grainTypeName()}\"" +
            //        $" AND (c.State.UserState.FtIndexedString = \"{ftValue(42)}\" OR c.State.NftIndexedInt = 5)",
            //    requestOptions: new QueryRequestOptions
            //    {
            //        MaxItemCount = -1,
            //        MaxConcurrency = -1,
            //        EnableScanInQuery = true
            //    }
            //);

            //FeedResponse<dynamic> result = await query.ReadNextAsync();

            
            // This should return a dictionary containing a single QueryMetrics item.
            // IReadOnlyDictionary<string, QueryMetrics> metrics = result.QueryMetrics;
            //Assert.Single(metrics);
            //Assert.Equal((max / mod + 1) * (1.0 / max), metrics["0"].IndexHitRatio);    // IndexHitDocumentCount is not public
        }
    }
}
