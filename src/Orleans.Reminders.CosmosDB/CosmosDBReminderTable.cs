using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Reminders.CosmosDB.Models;
using Orleans.Reminders.CosmosDB.Options;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;

namespace Orleans.Reminders.CosmosDB
{
    internal class CosmosDBReminderTable : IReminderTable
    {
        private const string PARTITION_KEY = "/PartitionKey";
        private readonly IGrainReferenceConverter _grainReferenceConverter;
        private readonly ILogger _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly SiloOptions _siloOptions;
        private readonly AzureCosmosDBReminderProviderOptions _options;
        private IDocumentClient _dbClient;

        public CosmosDBReminderTable(IGrainReferenceConverter grainReferenceConverter,
            ILoggerFactory loggerFactory,
            IOptions<SiloOptions> siloOptions,
            IOptions<AzureCosmosDBReminderProviderOptions> options)
        {
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory.CreateLogger(nameof(CosmosDBReminderTable));
            this._siloOptions = siloOptions.Value;
            this._options = options.Value;
        }

        public async Task Init(GlobalConfiguration config)
        {
            var dbClient = new DocumentClient(new Uri(this._options.AccountEndpoint), this._options.AccountKey,
                    new ConnectionPolicy
                    {
                        ConnectionMode = this._options.ConnectionMode,
                        ConnectionProtocol = this._options.ConnectionProtocol
                    });

            await dbClient.OpenAsync();

            if (this._options.CanCreateResources)
            {
                await dbClient.CreateDatabaseIfNotExistsAsync(new Database { Id = this._options.DB });

                var clusterCollection = new DocumentCollection
                {
                    Id = this._options.Collection
                };
                clusterCollection.PartitionKey.Paths.Add(PARTITION_KEY);
                // TODO: Set indexing policy to the collection

                await dbClient.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(this._options.DB),
                    clusterCollection,
                    new RequestOptions
                    {
                        PartitionKey = new PartitionKey(PARTITION_KEY),
                        //TODO: Check the consistency level for the emulator
                        //ConsistencyLevel = ConsistencyLevel.Strong,
                        OfferThroughput = this._options.CollectionThroughput
                    });
            }

            this._dbClient = dbClient;
        }

        public async Task<ReminderEntry> ReadRow(GrainReference grainRef, string reminderName)
        {
            try
            {
                var pk = new ReminderEntityPartitionKey
                {
                    ServiceId = this._siloOptions.ServiceId,
                    GrainRefConsistentHash = $"{grainRef.GetUniformHashCode():X8}"
                };

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<ReminderEntity>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "GetReminder"),
                        new RequestOptions { PartitionKey = new PartitionKey(pk) },
                        grainRef, reminderName).ConfigureAwait(false);

                ReminderEntry entry = null;

                if (spResponse.Response != null)
                {
                    ReminderEntity entity = spResponse.Response;
                    entry = new ReminderEntry
                    {
                        GrainRef = entity.GrainReference,
                        ReminderName = entity.Name,
                        StartAt = entity.StartAt.DateTime,
                        Period = entity.Period,
                        ETag = entity.ETag
                    };
                }

                return entry;
            }
            catch (Exception exc)
            {
                throw;
            }
        }

        public Task<ReminderTableData> ReadRows(GrainReference key)
        {
            try
            {
                var pk = new ReminderEntityPartitionKey
                {
                    ServiceId = this._siloOptions.ServiceId,
                    GrainRefConsistentHash = $"{key.GetUniformHashCode():X8}"
                };

                return null;
            }
            catch (Exception exc)
            {
                throw;
            }
        }

        public Task<ReminderTableData> ReadRows(uint begin, uint end)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RemoveRow(GrainReference grainRef, string reminderName, string eTag)
        {
            throw new NotImplementedException();
        }

        public Task TestOnlyClearTable()
        {
            throw new NotImplementedException();
        }

        public Task<string> UpsertRow(ReminderEntry entry)
        {
            throw new NotImplementedException();
        }
    }
}
