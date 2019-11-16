// TODO: ENABLE THIS AFTER FIGURE OUT HOW TODO IT ON THE NEW SDK

//using System;
//using Orleans.CosmosDB.Tests.Grains;
//using Orleans.Hosting;
//using Orleans.Persistence.CosmosDB;
//using Orleans.Runtime;
//using Orleans.Storage;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using Xunit;
//using static Orleans.CosmosDB.Tests.ThroughputConfigurationTests;

//// For Index coverage CreateDocumentQuery
//using System.Net.Http;
//using Microsoft.Azure.Cosmos;

//namespace Orleans.CosmosDB.Tests
//{
//    public class ThroughputConfigurationTests : IClassFixture<StorageFixture>
//    {
//        //private const string StorageDbName = "OrleansStorageTest";
//        private const string DatabaseName = "DatabaseRUTest";
//        private StorageFixture _fixture;

//        public class StorageFixture : OrleansFixture
//        {
//            internal string AccountEndpoint;
//            internal string AccountKey;

//            protected override ISiloBuilder PreBuild(ISiloBuilder builder)
//            {
//                OrleansFixture.GetAccountInfo(out this.AccountEndpoint, out this.AccountKey);

//                var httpHandler = new HttpClientHandler()
//                {
//                    ServerCertificateCustomValidationCallback = (req, cert, chain, errors) => true
//                };

//                var dbClient = new CosmosClient(
//                   this.AccountEndpoint,
//                   this.AccountKey,
//                   new CosmosClientOptions { ConnectionMode = ConnectionMode.Gateway }
//               );

//                return builder
//                    .AddCosmosDBGrainStorage(OrleansFixture.TEST_STORAGE, opt =>
//                    {
//                        opt.Client = dbClient;
//                        opt.DropDatabaseOnInit = true;
//                        opt.CanCreateResources = true;
//                        opt.DB = DatabaseName;
//                        opt.DatabaseThroughput = 1000;
//                        opt.CollectionThroughput = 0;
//                        opt.Collection = "RUTest";
//                    })
//                    .AddCosmosDBGrainStorage("Second", opt =>
//                    {
//                        opt.Client = dbClient;
//                        opt.DropDatabaseOnInit = true;
//                        opt.CanCreateResources = true;
//                        opt.DB = DatabaseName;
//                        opt.DatabaseThroughput = 1000;
//                        opt.CollectionThroughput = 500;
//                        opt.Collection = "RUTest2";
//                    });

//            }
//        }

//        public ThroughputConfigurationTests(StorageFixture fixture) => this._fixture = fixture;

//        [Fact]
//        public async Task VerifyDbThroughput()
//        {
//            var storage = this._fixture.Host.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
//            var dbClient = storage._cosmos;
//            var offers = dbClient.CreateOfferQuery().ToList();

//            //Database has offer
//            var database = (Database)(await dbClient.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName)));
//            var offerDatabase = (OfferV2)offers.Single(o => o.ResourceLink == database.SelfLink);
//            Assert.Equal(1000, offerDatabase.Content.OfferThroughput);
//        }

//        [Fact]
//        public async Task VerifyCollectionWithoutOffer()
//        {
//            var storage = this._fixture.Host.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
//            var dbClient = storage._cosmos;
//            var offers = dbClient.CreateOfferQuery().ToList();

//            //Collection RUTest does not
//            var collection1 = (DocumentCollection)(await dbClient.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, "RUTest")));
//            var offerCollection1 = offers.FirstOrDefault(o => o.ResourceLink == collection1.SelfLink);
//            Assert.Null(offerCollection1);
//        }

//        [Fact]
//        public async Task VerifiyCollectionWithOfferInDbWithOffer()
//        {
//            var storage = this._fixture.Host.Services.GetServiceByName<IGrainStorage>(OrleansFixture.TEST_STORAGE) as CosmosDBGrainStorage;
//            var dbClient = storage._cosmos;
//            var offers = dbClient.CreateOfferQuery().ToList();

//            //Collection RUTest2 has offer
//            var collection2 = (DocumentCollection)(await dbClient.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, "RUTest2")));
//            var offerCollection2 = (OfferV2)offers.Single(o => o.ResourceLink == collection2.SelfLink);
//            Assert.Equal(500, offerCollection2.Content.OfferThroughput);
//        }
//    }
//}
