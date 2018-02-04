using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB;
using Orleans.Clustering.CosmosDB.Options;
using Orleans.Messaging;
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

        protected override IMembershipTable CreateMembershipTable(ILogger logger)
        {
            //TestUtils.CheckForAzureStorage();
            var options = new AzureCosmosDBClusteringOptions()
            {
                AccountEndpoint = "https://localhost:8081",
                AccountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                ClusterId = this.clusterId,
                CanCreateResources = true,
                AutoUpdateStoredProcedures = true,
                DropDatabaseOnInit = true,
                ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway,
                DB = "OrleansMBRTest"

            };
            return new CosmosDBMembershipTable(loggerFactory, Options.Create(options));
        }

        protected override IGatewayListProvider CreateGatewayListProvider(ILogger logger)
        {
            var options = new AzureCosmosDBGatewayOptions()
            {
                AccountEndpoint = "https://localhost:8081",
                AccountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                ClusterId = this.clusterId,
                ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode.Gateway,
                DB = "OrleansMBRTest"
            };
            return new CosmosDBGatewayListProvider(loggerFactory, Options.Create(options), this.clientConfiguration);
        }

        protected override Task<string> GetConnectionString()
        {
            //TestUtils.CheckForAzureStorage();
            return Task.FromResult("");
        }

        [Fact]
        public async Task MembershipTable_Azure_GetGateways()
        {
            await MembershipTable_GetGateways();
        }

        [Fact]
        public async Task MembershipTable_Azure_ReadAll_EmptyTable()
        {
            await MembershipTable_ReadAll_EmptyTable();
        }

        [Fact]
        public async Task MembershipTable_Azure_InsertRow()
        {
            await MembershipTable_InsertRow();
        }

        [Fact]
        public async Task MembershipTable_Azure_ReadRow_Insert_Read()
        {
            await MembershipTable_ReadRow_Insert_Read();
        }

        [Fact]
        public async Task MembershipTable_Azure_ReadAll_Insert_ReadAll()
        {
            await MembershipTable_ReadAll_Insert_ReadAll();
        }

        [Fact]
        public async Task MembershipTable_Azure_UpdateRow()
        {
            await MembershipTable_UpdateRow();
        }

        // TODO: Enable this after implement retry police
        // See https://blogs.msdn.microsoft.com/bigdatasupport/2015/09/02/dealing-with-requestratetoolarge-errors-in-azure-documentdb-and-testing-performance/ 
        //[Fact]
        //public async Task MembershipTable_Azure_UpdateRowInParallel()
        //{
        //    await MembershipTable_UpdateRowInParallel();
        //}

        [Fact]
        public async Task MembershipTable_Azure_UpdateIAmAlive()
        {
            await MembershipTable_UpdateIAmAlive();
        }
    }
}
