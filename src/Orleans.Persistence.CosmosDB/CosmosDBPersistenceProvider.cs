using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
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

namespace Orleans.Persistence.CosmosDB
{
    public class CosmosDBPersistenceProvider : IStorageProvider
    {
        private const string PARTITION_KEY = "/GrainType";

        private const string WRITE_STATE_SPROC = "WriteState";
        private const string READ_STATE_SPROC = "ReadState";
        private const string CLEAR_STATE_SPROC = "ClearState";

        private readonly Dictionary<string, string> _sprocFiles;

        private ILoggerFactory _loggerFactory;
        private ILogger _logger;
        private Guid _serviceId;
        private AzureCosmosDBPersistenceProviderOptions _options;
        private DocumentClient _dbClient;

        public Logger Log { get; private set; }
        public string Name { get; private set; }

        public CosmosDBPersistenceProvider()
        {
            this._sprocFiles = new Dictionary<string, string>
            {
                { WRITE_STATE_SPROC, "WriteState.js" },
                { READ_STATE_SPROC, "ReadState.js" },
                { CLEAR_STATE_SPROC, "ClearState.js" }
            };
        }

        public async Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            this.Name = name;
            this._serviceId = providerRuntime.ServiceId;
            this._options = providerRuntime.ServiceProvider.GetRequiredService<IOptions<AzureCosmosDBPersistenceProviderOptions>>().Value;
            this._loggerFactory = providerRuntime.ServiceProvider.GetRequiredService<ILoggerFactory>();
            this._logger = this._loggerFactory.CreateLogger(nameof(CosmosDBPersistenceProvider));

            this._dbClient = new DocumentClient(new Uri(this._options.AccountEndpoint), this._options.AccountKey,
                    new ConnectionPolicy
                    {
                        ConnectionMode = this._options.ConnectionMode,
                        ConnectionProtocol = this._options.ConnectionProtocol
                    });

            await this._dbClient.OpenAsync();

            if (this._options.CanCreateResources)
            {
                if (this._options.DropDatabaseOnInit)
                {
                    await TryDeleteDatabase();
                }

                await TryCreateCosmosDBResources();

                if (this._options.AutoUpdateStoredProcedures)
                {
                    await UpdateStoredProcedures();
                }
            }
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            string id = GetKeyString(grainReference);

            try
            {
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<GrainStateEntity>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_STATE_SPROC),
                        new RequestOptions { PartitionKey = new PartitionKey(grainType) },
                        grainType, id)).ConfigureAwait(false);

                if (spResponse.Response?.State != null)
                {
                    grainState.State = ((JObject)spResponse.Response.State).ToObject(grainState.State.GetType()) ?? Activator.CreateInstance(grainState.State.GetType());
                    grainState.ETag = spResponse.Response.ETag;
                }
                else
                {
                    // Default state, to prevent null reference exceptions when the grain is first activated
                    grainState.State = Activator.CreateInstance(grainState.State.GetType());
                }
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    // State is new, just activate a default and return
                    grainState.State = Activator.CreateInstance(grainState.State.GetType());
                    return;
                }

                this._logger.LogError(dce, $"Failure reading state for Grain Type {grainType} with Id {id}.");
                throw dce;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading state for Grain Type {grainType} with Id {id}.");
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

                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<string>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, WRITE_STATE_SPROC),
                       new RequestOptions { PartitionKey = new PartitionKey(grainType) },
                       entity)).ConfigureAwait(false);

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
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<string>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, CLEAR_STATE_SPROC),
                        new RequestOptions { PartitionKey = new PartitionKey(grainType) },
                        grainType, id, grainState.ETag, this._options.DeleteOnClear)).ConfigureAwait(false);

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
            this._dbClient.Dispose();
            return Task.CompletedTask;
        }

        private static async Task<TResult> ExecuteWithRetries<TResult>(Func<Task<TResult>> clientFunc)
        {
            // From:  https://blogs.msdn.microsoft.com/bigdatasupport/2015/09/02/dealing-with-requestratetoolarge-errors-in-azure-documentdb-and-testing-performance/

            TimeSpan sleepTime = TimeSpan.Zero;

            while (true)
            {
                try
                {
                    return await clientFunc();
                }
                catch (DocumentClientException dce)
                {
                    if ((int)dce.StatusCode != 429)
                    {
                        throw;
                    }
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    DocumentClientException dce = (DocumentClientException)ae.InnerException;
                    if ((int)dce.StatusCode != 429)
                    {
                        throw;
                    }

                    sleepTime = dce.RetryAfter;

                    await Task.Delay(sleepTime);
                }
            }
        }

        private string GetKeyString(GrainReference grainReference) => $"{this._serviceId}_{grainReference.ToKeyString()}";

        private async Task TryCreateCosmosDBResources()
        {
            await this._dbClient.CreateDatabaseIfNotExistsAsync(new Database { Id = this._options.DB });

            var clusterCollection = new DocumentCollection
            {
                Id = this._options.Collection
            };
            clusterCollection.PartitionKey.Paths.Add(PARTITION_KEY);
            // TODO: Set indexing policy to the collection

            await this._dbClient.CreateDocumentCollectionIfNotExistsAsync(
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

        private async Task TryDeleteDatabase()
        {
            try
            {
                var dbUri = UriFactory.CreateDatabaseUri(this._options.DB);
                await this._dbClient.ReadDatabaseAsync(dbUri);
                await this._dbClient.DeleteDatabaseAsync(dbUri);
            }
            catch(DocumentClientException dce)
            {
                if(dce.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return;
                }
                else
                {
                    throw;
                }
            }
        }

        private async Task UpdateStoredProcedures()
        {
            var assembly = Assembly.GetExecutingAssembly();
            foreach (var sproc in this._sprocFiles.Keys)
            {
                using (var fileStream = assembly.GetManifestResourceStream($"Orleans.Persistence.CosmosDB.Sprocs.{this._sprocFiles[sproc]}"))
                using (var reader = new StreamReader(fileStream))
                {
                    var content = await reader.ReadToEndAsync();
                    await UpdateStoredProcedure(sproc, content);
                }
            }
        }

        private async Task UpdateStoredProcedure(string name, string content)
        {
            // Partitioned Collections do not support upserts, so check if they exist, and delete/re-insert them if they've changed.
            var insertStoredProc = false;

            try
            {
                var storedProcUri = UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, name);
                var storedProcResponse = await this._dbClient.ReadStoredProcedureAsync(storedProcUri);
                var storedProc = storedProcResponse.Resource;

                if (storedProc == null || !Equals(storedProc.Body, content))
                {
                    insertStoredProc = true;
                    await this._dbClient.DeleteStoredProcedureAsync(storedProcUri);
                }
            }
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == HttpStatusCode.NotFound)
                {
                    insertStoredProc = true;
                }
                else
                {
                    throw;
                }
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure Updating Stored Procecure {name}");
                throw;
            }

            if (insertStoredProc)
            {
                var newStoredProc = new StoredProcedure()
                {
                    Id = name,
                    Body = content
                };

                await this._dbClient.CreateStoredProcedureAsync(UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection), newStoredProc);
            }
        }
    }
}
