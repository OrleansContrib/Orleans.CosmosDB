using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using Orleans.Persistence.CosmosDB.Models;
using Orleans.Persistence.CosmosDB.Options;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;
using System;
using System.Threading.Tasks;

namespace Orleans.Persistence.CosmosDB
{
    public class CosmosDBPersistenceProvider : IStorageProvider
    {
        private const string PARTITION_KEY = "/GrainType";
        private ILoggerFactory _loggerFactory;
        private ILogger _logger;
        private Guid _serviceId;
        private AzureCosmosDBPersistenceProviderOptions _options;
        private IDocumentClient _dbClient;

        public Logger Log { get; private set; }
        public string Name { get; private set; }

        public async Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            this.Name = name;
            this._serviceId = providerRuntime.ServiceId;
            this._options = providerRuntime.ServiceProvider.GetRequiredService<IOptions<AzureCosmosDBPersistenceProviderOptions>>().Value;
            this._loggerFactory = providerRuntime.ServiceProvider.GetRequiredService<ILoggerFactory>();
            this._logger = this._loggerFactory.CreateLogger(nameof(CosmosDBPersistenceProvider));

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

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                string id = GetKeyString(grainReference);

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<GrainStateEntity>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "ReadState"),
                        new RequestOptions { PartitionKey = new PartitionKey(grainType) },
                        grainType, id).ConfigureAwait(false);

                if (spResponse.Response?.State != null)
                {
                    grainState.State = ((JObject)spResponse.Response.State).ToObject(grainState.State.GetType()) ?? Activator.CreateInstance(grainState.State.GetType());
                    grainState.ETag = spResponse.Response.ETag;
                }
            }
            catch (Exception exc)
            {

                throw;
            }
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            string id = GetKeyString(grainReference);

            try
            {
                var entity = new GrainStateEntity
                {
                    ETag = grainState.ETag,
                    Id = id,
                    GrainType = grainType,
                    State = grainState.State
                };

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<string>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "WriteState"),
                        new RequestOptions { PartitionKey = new PartitionKey(grainType) },
                        entity).ConfigureAwait(false);

                grainState.ETag = spResponse.Response;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure writing state for Grain Type {grainType} with Id {id}.");
                throw;
            }
        }

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            string id = GetKeyString(grainReference);

            try
            {
                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<string>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "ClearState"),
                        new RequestOptions { PartitionKey = new PartitionKey(grainType) },
                        grainType, id, grainState.ETag, this._options.DeleteOnClear).ConfigureAwait(false);

                grainState.ETag = spResponse.Response;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure clearing state for Grain Type {grainType} with Id {id}.");
                throw;
            }
        }

        public Task Close()
        {
            return Task.CompletedTask;
        }

        private string GetKeyString(GrainReference grainReference) => $"{this._serviceId}_{grainReference.ToKeyString()}";
    }
}
