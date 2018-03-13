using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using Orleans.Configuration;
using Orleans.Persistence.CosmosDB.Models;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Persistence.CosmosDB
{
    /// <summary>
    /// Azure Cosmos DB storage Provider.
    /// Persist Grain State in a Cosmos DB collection.
    /// </summary>
    public class CosmosDBGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private const string PARTITION_KEY = "/GrainType";
        private const string WRITE_STATE_SPROC = "WriteState";
        private const string READ_STATE_SPROC = "ReadState";
        private const string CLEAR_STATE_SPROC = "ClearState";

        private readonly Dictionary<string, string> _sprocFiles;

        private readonly Guid _serviceId;
        private readonly string _name;
        private readonly SerializationManager _serializationManager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly IGrainFactory _grainFactory;
        private readonly ITypeResolver _typeResolver;
        private readonly CosmosDBStorageOptions _options;
        private DocumentClient _dbClient;

        public CosmosDBGrainStorage(string name, CosmosDBStorageOptions options, SerializationManager serializationManager,
            IOptions<ClusterOptions> clusterOptions, IGrainFactory grainFactory, ITypeResolver typeResolver, ILoggerFactory loggerFactory)
        {
            this._name = name;

            this._loggerFactory = loggerFactory;
            var loggerName = $"{typeof(CosmosDBGrainStorage).FullName}.{name}";
            this._logger = loggerFactory.CreateLogger(loggerName);
            this._options = options;
            this._serializationManager = serializationManager;
            this._grainFactory = grainFactory;
            this._typeResolver = typeResolver;
            this._serviceId = clusterOptions.Value.ServiceId;

            this._sprocFiles = new Dictionary<string, string>
            {
                { WRITE_STATE_SPROC, $"{WRITE_STATE_SPROC}.js" },
                { READ_STATE_SPROC, $"{READ_STATE_SPROC}.js" },
                { CLEAR_STATE_SPROC, $"{CLEAR_STATE_SPROC}.js" }
            };
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<CosmosDBGrainStorage>(this._name), this._options.InitStage, this.Init, this.Close);
        }

        public async Task Init(CancellationToken ct)
        {
            var stopWatch = Stopwatch.StartNew();

            try
            {
                var initMsg = string.Format("Init: Name={0} ServiceId={1} Collection={2} DeleteStateOnClear={3}",
                        this._name, this._serviceId, this._options.Collection, this._options.DeleteStateOnClear);

                this._logger.LogInformation($"Azure Cosmos DB Grain Storage {this._name} is initializing: {initMsg}");

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

                stopWatch.Stop();
                this._logger.LogInformation(
                    $"Initializing provider {this._name} of type {this.GetType().Name} in stage {this._options.InitStage} took {stopWatch.ElapsedMilliseconds} Milliseconds.");
            }
            catch (Exception exc)
            {
                stopWatch.Stop();
                this._logger.LogError($"Initialization failed for provider {this._name} of type {this.GetType().Name} in stage {this._options.InitStage} in {stopWatch.ElapsedMilliseconds} Milliseconds.", exc);
                throw;
            }
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (this._dbClient == null) throw new ArgumentException("GrainState collection not initialized.");

            string id = GetKeyString(grainReference);

            if (this._logger.IsEnabled(LogLevel.Trace)) this._logger.Trace(
                "Reading: GrainType={0} Key={1} Grainid={2} from Collection={3}",
                grainType, id, grainReference, this._options.Collection);

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
            if (this._dbClient == null) throw new ArgumentException("GrainState collection not initialized.");

            string id = GetKeyString(grainReference);

            if (this._logger.IsEnabled(LogLevel.Trace)) this._logger.Trace(
                "Writing: GrainType={0} Key={1} Grainid={2} ETag={3} from Collection={4}",
                grainType, id, grainReference, grainState.ETag, this._options.Collection);

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
            if (this._dbClient == null) throw new ArgumentException("GrainState collection not initialized.");

            string id = GetKeyString(grainReference);

            if (this._logger.IsEnabled(LogLevel.Trace)) this._logger.Trace(
                "Clearing: GrainType={0} Key={1} Grainid={2} ETag={3} DeleteStateOnClear={4} from Collection={4}",
                grainType, id, grainReference, grainState.ETag, this._options.DeleteStateOnClear, this._options.Collection);

            try
            {
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<string>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, CLEAR_STATE_SPROC),
                        new RequestOptions { PartitionKey = new PartitionKey(grainType) },
                        grainType, id, grainState.ETag, this._options.DeleteStateOnClear)).ConfigureAwait(false);

                grainState.ETag = spResponse.Response;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure clearing state for Grain Type {grainType} with Id {id}.");
                throw;
            }
        }

        public Task Close(CancellationToken ct)
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

            var stateCollection = new DocumentCollection
            {
                Id = this._options.Collection
            };
            stateCollection.PartitionKey.Paths.Add(PARTITION_KEY);

            stateCollection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            stateCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            stateCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/\"State\"/*" });

            if (this._options.StateFieldsToIndex != null)
            {
                foreach (var idx in this._options.StateFieldsToIndex)
                {
                    var path = idx.StartsWith("/") ? $"/State{idx}" : $"/State/{idx}";
                    stateCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = path });
                }
            }

            await this._dbClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(this._options.DB),
                stateCollection,
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
            catch (DocumentClientException dce)
            {
                if (dce.StatusCode == System.Net.HttpStatusCode.NotFound)
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

    public static class CosmosDBGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<CosmosDBStorageOptions> optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<CosmosDBStorageOptions>>();
            return ActivatorUtilities.CreateInstance<CosmosDBGrainStorage>(services, optionsSnapshot.Get(name), name);
        }
    }
}
