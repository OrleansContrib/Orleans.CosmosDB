using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Persistence.CosmosDB.Models;
using Orleans.Persistence.CosmosDB.Options;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
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
        private const string LOOKUP_INDEX_SPROC = "LookupIndex";
        private const string DEFAULT_PARTITION_KEY_PATH = "/PartitionKey";
        private const string GRAINTYPE_PARTITION_KEY_PATH = "/GrainType";

        private readonly Dictionary<string, string> _sprocFiles;

        private readonly string _serviceId;
        private readonly string _name;
        private readonly SerializationManager _serializationManager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly IGrainFactory _grainFactory;
        private readonly ITypeResolver _typeResolver;
        private readonly CosmosDBStorageOptions _options;
        private readonly IPartitionKeyProvider _partitionKeyProvider;
        internal DocumentClient _dbClient;  // internal for test


        private string _partitionKeyPath = DEFAULT_PARTITION_KEY_PATH; //overwritten with old value for existing collections

        private IGrainReferenceConverter _grainReferenceConverter;

        private const HttpStatusCode TooManyRequests = (HttpStatusCode)429;

        public CosmosDBGrainStorage(string name, CosmosDBStorageOptions options, SerializationManager serializationManager,
            Providers.IProviderRuntime providerRuntime, IPartitionKeyProvider partitionKeyProvider,
            IOptions<ClusterOptions> clusterOptions, IGrainFactory grainFactory, ITypeResolver typeResolver, ILoggerFactory loggerFactory)
        {
            this._name = name;
            this._partitionKeyProvider = partitionKeyProvider;
            this._loggerFactory = loggerFactory;
            var loggerName = $"{typeof(CosmosDBGrainStorage).FullName}.{name}";
            this._logger = loggerFactory.CreateLogger(loggerName);
            this._options = options;
            this._serializationManager = serializationManager;
            this._grainFactory = grainFactory;
            this._typeResolver = typeResolver;
            this._serviceId = clusterOptions.Value.ServiceId;
            this._grainReferenceConverter = providerRuntime.ServiceProvider.GetRequiredService<IGrainReferenceConverter>();

            this._sprocFiles = new Dictionary<string, string>
            {
                { LOOKUP_INDEX_SPROC, $"{LOOKUP_INDEX_SPROC}.js" }
            };

            if (this._options.JsonSerializerSettings == null)
            {
                this._options.JsonSerializerSettings = OrleansJsonSerializer.UpdateSerializerSettings(OrleansJsonSerializer.GetDefaultSerializerSettings(this._typeResolver, this._grainFactory),
                    this._options.UseFullAssemblyNames,
                    this._options.IndentJson,
                    this._options.TypeNameHandling);
                this._options.JsonSerializerSettings.DefaultValueHandling = DefaultValueHandling.Include;
                this._options.JsonSerializerSettings.PreserveReferencesHandling = PreserveReferencesHandling.None;
            }
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

                if (this._options.Client != null)
                {
                    this._dbClient = this._options.Client;
                }
                else
                {
                    this._dbClient = new DocumentClient(new Uri(this._options.AccountEndpoint), this._options.AccountKey,
                            new ConnectionPolicy
                            {
                                ConnectionMode = this._options.ConnectionMode,
                                ConnectionProtocol = this._options.ConnectionProtocol
                            });
                }

                await this._dbClient.OpenAsync();

                if (this._options.CanCreateResources)
                {
                    if (this._options.DropDatabaseOnInit)
                    {
                        await TryDeleteDatabase();
                    }

                    await TryCreateCosmosDBResources();
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
            string partitionKey = await BuildPartitionKey(grainType, grainReference);

            if (this._logger.IsEnabled(LogLevel.Trace)) this._logger.Trace(
                "Reading: GrainType={0} Key={1} Grainid={2} from Collection={3} with PartitionKey={4}",
                grainType, id, grainReference, this._options.Collection, partitionKey);

            try
            {
                var doc = await ExecuteWithRetries(async () => await this._dbClient.ReadDocumentAsync<GrainStateEntity>(
                    UriFactory.CreateDocumentUri(this._options.DB, this._options.Collection, id),
                    new RequestOptions { PartitionKey = new PartitionKey(partitionKey) })).ConfigureAwait(false);

                if (doc.Document?.State != null)
                {
                    grainState.State = JsonConvert.DeserializeObject(doc.Document.State.ToString(), grainState.State.GetType(), this._options.JsonSerializerSettings);
                    grainState.ETag = doc.Document.ETag;
                }
                else
                {
                    // Default state, to prevent null reference exceptions when the grain is first activated
                    grainState.State = Activator.CreateInstance(grainState.State.GetType());
                    grainState.ETag = doc.Document?.ETag;
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

            string partitionKey = await BuildPartitionKey(grainType, grainReference);

            if (this._logger.IsEnabled(LogLevel.Trace)) this._logger.Trace(
                "Writing: GrainType={0} Key={1} Grainid={2} ETag={3} from Collection={4} with PartitionKey={5}",
                grainType, id, grainReference, grainState.ETag, this._options.Collection, partitionKey);

            ResourceResponse<Document> response = null;

            try
            {
                var entity = new GrainStateEntity
                {
                    ETag = grainState.ETag,
                    Id = id,
                    GrainType = grainType,
                    State = grainState.State,
                    PartitionKey = partitionKey
                };

                if (string.IsNullOrWhiteSpace(grainState.ETag))
                {
                    response = await ExecuteWithRetries(() => this._dbClient.CreateDocumentAsync(
                       UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                       entity,
                       new RequestOptions { PartitionKey = new PartitionKey(partitionKey) },
                       true)).ConfigureAwait(false);

                    grainState.ETag = response.Resource.ETag;
                }
                else
                {
                    var requestOptions = new RequestOptions
                    {
                        PartitionKey = new PartitionKey(partitionKey),
                        AccessCondition = new AccessCondition { Condition = grainState.ETag, Type = AccessConditionType.IfMatch }
                    };

                    response = await ExecuteWithRetries(() =>
                        this._dbClient.ReplaceDocumentAsync(
                            UriFactory.CreateDocumentUri(this._options.DB, this._options.Collection, entity.Id),
                            entity, requestOptions)).ConfigureAwait(false);
                    grainState.ETag = response.Resource.ETag;
                }

            }
            catch (DocumentClientException dce) when (dce.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                throw new CosmosConditionNotSatisfiedException(grainType, grainReference, this._options.Collection, "Unknown", grainState.ETag, dce);
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
            string partitionKey = await BuildPartitionKey(grainType, grainReference);
            if (this._logger.IsEnabled(LogLevel.Trace)) this._logger.Trace(
                "Clearing: GrainType={0} Key={1} Grainid={2} ETag={3} DeleteStateOnClear={4} from Collection={4} with PartitionKey {5}",
                grainType, id, grainReference, grainState.ETag, this._options.DeleteStateOnClear, this._options.Collection, partitionKey);

            var requestOptions = new RequestOptions
            {
                PartitionKey = new PartitionKey(partitionKey),
                AccessCondition = new AccessCondition { Condition = grainState.ETag, Type = AccessConditionType.IfMatch }
            };

            try
            {
                if (this._options.DeleteStateOnClear)
                {
                    if (string.IsNullOrWhiteSpace(grainState.ETag))
                        return;  //state not written

                    await ExecuteWithRetries(() => this._dbClient.DeleteDocumentAsync(
                        UriFactory.CreateDocumentUri(this._options.DB, this._options.Collection, id),
                        requestOptions));
                }
                else
                {
                    var entity = new GrainStateEntity
                    {
                        ETag = grainState.ETag,
                        Id = id,
                        GrainType = grainType,
                        State = null,
                        PartitionKey = partitionKey
                    };

                    var response = await ExecuteWithRetries(() =>
                        string.IsNullOrWhiteSpace(grainState.ETag)
                        ? this._dbClient.CreateDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                            entity,
                            new RequestOptions { PartitionKey = new PartitionKey(partitionKey) })
                        : this._dbClient.ReplaceDocumentAsync(
                            UriFactory.CreateDocumentUri(this._options.DB, this._options.Collection, entity.Id),
                            entity, requestOptions)).ConfigureAwait(false);
                    grainState.ETag = response.Resource.ETag;
                }
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure clearing state for Grain Type {grainType} with Id {id}.");
                throw;
            }
        }

        // This method is invoked via "dynamic" by Orleans.Indexing. A future interface may be designed for this.
        public async Task<List<GrainReference>> LookupAsync<K>(string grainType, string indexedField, K key)
        {
            if (this._dbClient == null) throw new ArgumentException("GrainState collection not initialized.");

            //indexing is not supported if cosmosDb is configured with custom partition key builders. For this to be
            //supported the index should be updated from DirectStorageManagedIndexImpl class rather than implicit through
            //write state.
            if (!(this._partitionKeyProvider is DefaultPartitionKeyProvider))
                throw new NotSupportedException("Indexing is not supported with custom partition key builders");

            var keyString = key.ToString();
            if (!IsNumericType(typeof(K)))
            {
                keyString = $"\"{keyString}\"";
            }
            var logMessage = $"GrainType={grainType} IndexedField={indexedField} Key={keyString} from Collection={this._options.Collection}";
            if (this._logger.IsEnabled(LogLevel.Trace)) this._logger.Trace($"Reading: {logMessage}");

            try
            {
                var response = await ExecuteWithRetries(async () =>
                {
                    var pk = new PartitionKey(grainType);
                    var sqlParams = new SqlParameterCollection();
                    sqlParams.Add(new SqlParameter("@key", key));
                    var sqlSpec = new SqlQuerySpec($"SELECT * FROM c WHERE c.State.{indexedField} = @key", sqlParams);

                    var query = this._dbClient.CreateDocumentQuery<GrainStateEntity>(
                        UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                        sqlSpec,
                        new FeedOptions { PartitionKey = pk }
                    ).AsDocumentQuery();

                    var reminders = new List<GrainStateEntity>();

                    do
                    {
                        var queryResponse = await query.ExecuteNextAsync<GrainStateEntity>().ConfigureAwait(false);
                        if (queryResponse != null && queryResponse.Count > 0)
                        {
                            reminders.AddRange(queryResponse.ToArray());
                        }
                        else
                        {
                            break;
                        }
                    } while (query.HasMoreResults);

                    return reminders;
                }).ConfigureAwait(false);

                return response.Select(entity => this._grainReferenceConverter.GetGrainFromKeyString(GetGrainReferenceString(entity.Id))).ToList();
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading state for {logMessage}.");
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
            while (true)
            {
                var sleepTime = TimeSpan.Zero;
                try
                {
                    return await clientFunc();
                }
                catch (DocumentClientException dce) when (dce.StatusCode == TooManyRequests)
                {
                    sleepTime = dce.RetryAfter;
                }
                catch (AggregateException ae) when ((ae.InnerException is DocumentClientException dce) && dce.StatusCode == TooManyRequests)
                {
                    sleepTime = dce.RetryAfter;
                }
                await Task.Delay(sleepTime);
            }
        }

        private const string KeyStringSeparator = "__";
        private string GetKeyString(GrainReference grainReference) => $"{this._serviceId}{KeyStringSeparator}{grainReference.ToKeyString()}";
        private string GetGrainReferenceString(string keyString) => keyString.Substring(keyString.IndexOf(KeyStringSeparator) + KeyStringSeparator.Length);
        private ValueTask<string> BuildPartitionKey(string grainType, GrainReference reference) =>
            this._partitionKeyProvider.GetPartitionKey(grainType, reference);

        private static bool IsNumericType(Type o)
        {
            switch (Type.GetTypeCode(o))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }

        private async Task TryCreateCosmosDBResources()
        {
            RequestOptions databaseOptions = new RequestOptions
            {
                OfferThroughput =
                    this._options.DatabaseThroughput >= 400
                    ? (int?)this._options.DatabaseThroughput
                    : null
            };
            var dbResponse = await this._dbClient.CreateDatabaseIfNotExistsAsync(new Database { Id = this._options.DB }, databaseOptions);

            var stateCollection = new DocumentCollection
            {
                Id = this._options.Collection
            };
            stateCollection.PartitionKey.Paths.Add(DEFAULT_PARTITION_KEY_PATH);

            stateCollection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            stateCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            stateCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/\"State\"/*" });

            if (this._options.StateFieldsToIndex != null)
            {
                foreach (var idx in this._options.StateFieldsToIndex)
                {
                    var path = idx.StartsWith("/") ? idx.Substring(1) : idx;
                    stateCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = $"/\"State\"/\"{idx}\"/?" });
                }
            }

            const int maxRetries = 3;
            for (var retry = 0; retry <= maxRetries; ++retry)
            {
                var collResponse = await this._dbClient.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(this._options.DB),
                    stateCollection,
                    new RequestOptions
                    {
                        PartitionKey = new PartitionKey(DEFAULT_PARTITION_KEY_PATH),
                        ConsistencyLevel = this._options.GetConsistencyLevel(),
                        OfferThroughput =
                            this._options.CollectionThroughput >= 400
                            ? (int?)this._options.CollectionThroughput
                            : null
                    });

                if (collResponse.StatusCode == HttpStatusCode.OK || collResponse.StatusCode == HttpStatusCode.Created)
                {
                    var documentCollection = (DocumentCollection)collResponse;
                    this._partitionKeyPath = documentCollection.PartitionKey.Paths.First();
                    if (this._partitionKeyPath == GRAINTYPE_PARTITION_KEY_PATH &&
                        !(this._partitionKeyProvider is DefaultPartitionKeyProvider))
                        throw new BadGrainStorageConfigException("Custom partition key provider is not compatible with partition key path set to /GrainType");
                }

                if (retry == maxRetries || dbResponse.StatusCode != HttpStatusCode.Created || collResponse.StatusCode == HttpStatusCode.Created)
                {
                    break;  // Apparently some throttling logic returns HttpStatusCode.OK (not 429) when the collection wasn't created in a new DB.
                }
                await Task.Delay(1000);
            }
        }

        private async Task TryDeleteDatabase()
        {
            try
            {
                var dbUri = UriFactory.CreateDatabaseUri(this._options.DB);
                await this._dbClient.ReadDatabaseAsync(dbUri);
                await this._dbClient.DeleteDatabaseAsync(dbUri);
            }
            catch (DocumentClientException dce) when (dce.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return;
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
