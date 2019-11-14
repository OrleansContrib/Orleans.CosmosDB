using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB;
using Orleans.Configuration;
using Orleans.Messaging;
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.CosmosDB.Tests
{
    /// <summary>
    /// Tests for operation of Orleans Membership Table using Azure Cosmos DB
    /// </summary>
    public class MBTTests : MembershipTableTestsBase/*, IClassFixture<AzureStorageBasicTests>*/
    {
        public MBTTests() : base(CreateFilters())
        {
        }

        private static LoggerFilterOptions CreateFilters()
        {
            var filters = new LoggerFilterOptions();
            //filters.AddFilter(typeof(Orleans.Clustering.CosmosDB.AzureTableDataManager<>).FullName, LogLevel.Trace);
            //filters.AddFilter(typeof(OrleansSiloInstanceManager).FullName, LogLevel.Trace);
            //filters.AddFilter("Orleans.Storage", LogLevel.Trace);
            return filters;
        }

        protected override IMembershipTable CreateMembershipTable(ILogger logger, string accountEndpoint, string accountKey)
        {
            var httpHandler = new HttpClientHandler()
            {
                ServerCertificateCustomValidationCallback = (req, cert, chain, errors) => true
            };

            var dbClient = new DocumentClient(new Uri(accountEndpoint), accountKey, httpHandler, new ConnectionPolicy { ConnectionMode = ConnectionMode.Gateway, ConnectionProtocol = Protocol.Https });

            //TestUtils.CheckForAzureStorage();
            var options = new CosmosDBClusteringOptions()
            {
                Client = dbClient,
                CanCreateResources = true,
                AutoUpdateStoredProcedures = true,
                DropDatabaseOnInit = true,
                DB = "OrleansMBRTest"
            };
            return new CosmosDBMembershipTable(this.loggerFactory, Options.Create(new ClusterOptions { ClusterId = this.clusterId }), Options.Create(options));
        }

        protected override IGatewayListProvider CreateGatewayListProvider(ILogger logger, string accountEndpoint, string accountKey)
        {
            var httpHandler = new HttpClientHandler()
            {
                ServerCertificateCustomValidationCallback = (req, cert, chain, errors) => true
            };

            var dbClient = new DocumentClient(new Uri(accountEndpoint), accountKey, httpHandler, new ConnectionPolicy { ConnectionMode = ConnectionMode.Gateway, ConnectionProtocol = Protocol.Https });

            var options = new CosmosDBGatewayOptions()
            {
                Client = dbClient,
                DB = "OrleansMBRTest"
            };
            return new CosmosDBGatewayListProvider(this.loggerFactory,
                Options.Create(options),
                Options.Create(new ClusterOptions { ClusterId = this.clusterId }),
                Options.Create(new GatewayOptions()));
        }

        protected override Task<string> GetConnectionString()
        {
            //TestUtils.CheckForAzureStorage();
            return Task.FromResult("");
        }

        [Fact]
        public async Task GetGateways()
        {
            await MembershipTable_GetGateways();
        }

        [Fact]
        public async Task ReadAll_EmptyTable()
        {
            await MembershipTable_ReadAll_EmptyTable();
        }

        [Fact]
        public async Task InsertRow()
        {
            await MembershipTable_InsertRow();
        }

        [Fact]
        public async Task ReadRow_Insert_Read()
        {
            await MembershipTable_ReadRow_Insert_Read();
        }

        [Fact]
        public async Task ReadAll_Insert_ReadAll()
        {
            await MembershipTable_ReadAll_Insert_ReadAll();
        }

        [Fact]
        public async Task UpdateRow()
        {
            await MembershipTable_UpdateRow();
        }

        // TODO: Enable this after implement retry police
        // See https://blogs.msdn.microsoft.com/bigdatasupport/2015/09/02/dealing-with-requestratetoolarge-errors-in-azure-documentdb-and-testing-performance/
        //[Fact]
        //public async Task UpdateRowInParallel()
        //{
        //    await MembershipTable_UpdateRowInParallel();
        //}

        [Fact]
        public async Task UpdateIAmAlive()
        {
            await MembershipTable_UpdateIAmAlive();
        }
    }
}
