using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Reminders.CosmosDB.Models;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Text;

namespace Orleans.Reminders.CosmosDB
{
    internal class CosmosDBReminderTable : IReminderTable
    {
        private const string READ_ROWS_QUERY = "SELECT * FROM c WHERE c.ServiceId = @serviceId";
        private const string READ_ROW_BY_ID_QUERY = "SELECT * FROM c WHERE c.id = @id";
        private const string READ_ROWS_BEGIN_LESS_THAN_END = " AND c.GrainHash > @begin AND c.GrainHash <= @end";
        private const string READ_ROWS_BEGIN_GREATER_THAN_END = " AND ((c.GrainHash > @begin) OR (c.GrainHash <= @end))";
        private const string PARTITION_KEY_PATH = "/PartitionKey";
        private readonly IGrainReferenceConverter _grainReferenceConverter;
        private readonly ILogger _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly CosmosDBReminderStorageOptions _options;
        private readonly string _serviceId;

        private DocumentClient _dbClient;

        private const HttpStatusCode TooManyRequests = (HttpStatusCode)429;

        public CosmosDBReminderTable(
            IGrainReferenceConverter grainReferenceConverter,
            ILoggerFactory loggerFactory,
            IOptions<ClusterOptions> _clusterOptions,
            IOptions<CosmosDBReminderStorageOptions> options)
        {
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory.CreateLogger(nameof(CosmosDBReminderTable));
            this._options = options.Value;
            this._grainReferenceConverter = grainReferenceConverter;
            this._serviceId = string.IsNullOrWhiteSpace(_clusterOptions.Value.ServiceId) ? Guid.Empty.ToString() : _clusterOptions.Value.ServiceId;
        }

        public async Task Init()
        {
            var stopWatch = Stopwatch.StartNew();

            try
            {
                var initMsg = string.Format("Init: Name={0} ServiceId={1} Collection={2}",
                        nameof(CosmosDBReminderTable), this._serviceId, this._options.Collection);

                this._logger.LogInformation($"Azure Cosmos DB Reminder Storage {nameof(CosmosDBReminderTable)} is initializing: {initMsg}");

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
                    $"Initializing {nameof(CosmosDBReminderTable)} took {stopWatch.ElapsedMilliseconds} Milliseconds.");
            }
            catch (Exception exc)
            {
                stopWatch.Stop();
                this._logger.LogError($"Initialization failed for provider {nameof(CosmosDBReminderTable)} in {stopWatch.ElapsedMilliseconds} Milliseconds.", exc);
                throw;
            }
        }

        public async Task<ReminderEntry> ReadRow(GrainReference grainRef, string reminderName)
        {
            try
            {
                var response = await ExecuteWithRetries(async () =>
                {
                    var pk = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, grainRef));
                    var sqlParams = new SqlParameterCollection();
                    sqlParams.Add(new SqlParameter("@id", ReminderEntity.ConstructId(grainRef, reminderName)));
                    var sqlSpec = new SqlQuerySpec(READ_ROW_BY_ID_QUERY, sqlParams);

                    var query = this._dbClient.CreateDocumentQuery<ReminderEntity>(
                        UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                        sqlSpec,
                        new FeedOptions { PartitionKey = pk }
                    ).AsDocumentQuery();

                    return (await query.ExecuteNextAsync<ReminderEntity>()).FirstOrDefault();
                }).ConfigureAwait(false);

                return response != null ? this.FromEntity(response) : null;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading reminder {reminderName} for service {this._serviceId} and grain {grainRef.ToKeyString()}.");
                throw;
            }
        }

        public async Task<ReminderTableData> ReadRows(GrainReference grainRef)
        {
            try
            {
                var response = await ExecuteWithRetries(async () =>
                {
                    var pk = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, grainRef));

                    var query = this._dbClient.CreateDocumentQuery<ReminderEntity>(
                        UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                        new FeedOptions
                        {
                            PartitionKey = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, grainRef))
                        }
                    ).AsDocumentQuery();

                    var reminders = new List<ReminderEntity>();
                    string continuation = string.Empty;
                    do
                    {
                        var queryResponse = await query.ExecuteNextAsync<ReminderEntity>();
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

                return new ReminderTableData(response.Select(this.FromEntity));
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading reminders for Grain Type {grainRef.InterfaceName} with Id {grainRef.ToKeyString()}.");
                throw;
            }
        }

        public async Task<ReminderTableData> ReadRows(uint begin, uint end)
        {
            try
            {
                var response = await ExecuteWithRetries(async () =>
                {
                    var sqlParams = new SqlParameterCollection();

                    var sql = new StringBuilder(READ_ROWS_QUERY);
                    sqlParams.Add(new SqlParameter("@serviceId", this._serviceId));
                    sqlParams.Add(new SqlParameter("@begin", begin));
                    sqlParams.Add(new SqlParameter("@end", end));

                    if (begin < end)
                    {
                        sql.Append(READ_ROWS_BEGIN_LESS_THAN_END);
                    }
                    else
                    {
                        sql.Append(READ_ROWS_BEGIN_GREATER_THAN_END);
                    }

                    var sqlSpec = new SqlQuerySpec(sql.ToString(), sqlParams);

                    var query = this._dbClient.CreateDocumentQuery<ReminderEntity>(
                        UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                        sqlSpec,
                        // Unless we find something else, we need this to make this range query
                        new FeedOptions { EnableCrossPartitionQuery = true }
                    );

                    var docQuery = query.AsDocumentQuery();
                    var reminders = new List<ReminderEntity>();

                    do
                    {
                        var queryResponse = await docQuery.ExecuteNextAsync<ReminderEntity>().ConfigureAwait(false);
                        if (queryResponse != null && queryResponse.Count > 0)
                        {
                            reminders.AddRange(queryResponse.ToArray());
                        }
                        else
                        {
                            break;
                        }
                    } while (docQuery.HasMoreResults);

                    return reminders;
                }).ConfigureAwait(false);

                return new ReminderTableData(response.Select(this.FromEntity));
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading reminders for Service {this._serviceId} for range {begin} to {end}.");
                throw;
            }
        }

        public async Task<bool> RemoveRow(GrainReference grainRef, string reminderName, string eTag)
        {
            try
            {
                var response = await ExecuteWithRetries(() =>
                {
                    var ac = new AccessCondition { Condition = eTag, Type = AccessConditionType.IfMatch };
                    var pk = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, grainRef));

                    return this._dbClient.DeleteDocumentAsync(
                        UriFactory.CreateDocumentUri(this._options.DB, this._options.Collection, ReminderEntity.ConstructId(grainRef, reminderName)),
                        new RequestOptions { AccessCondition = ac, PartitionKey = pk }
                    );
                }).ConfigureAwait(false);

                return true;
            }
            catch (DocumentClientException dce) when (dce.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                return false;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure removing reminders for Service {this._serviceId} with grainId {grainRef.ToKeyString()} and name {reminderName}.");
                throw;
            }
        }

        public async Task TestOnlyClearTable()
        {
            try
            {
                var entities = await ExecuteWithRetries(async () =>
                {
                    var query = this._dbClient.CreateDocumentQuery<ReminderEntity>(
                        UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                        new FeedOptions { EnableCrossPartitionQuery = true }
                    ).AsDocumentQuery();

                    var reminders = new List<ReminderEntity>();
                    do
                    {
                        var queryResponse = await query.ExecuteNextAsync<ReminderEntity>().ConfigureAwait(false);
                        if (queryResponse != null && queryResponse.Count > 0)
                        {
                            reminders.AddRange(queryResponse);
                        }
                        else
                        {
                            break;
                        }
                    } while (query.HasMoreResults);

                    return reminders;
                }).ConfigureAwait(false);

                var deleteTasks = new List<Task>();
                foreach (var entity in entities)
                {
                    deleteTasks.Add(ExecuteWithRetries(() =>
                    {
                        return this._dbClient.DeleteDocumentAsync(
                            UriFactory.CreateDocumentUri(this._options.DB, this._options.Collection, entity.Id),
                            new RequestOptions { PartitionKey = new PartitionKey(entity.PartitionKey) }
                        );
                    }));
                }
                await Task.WhenAll(deleteTasks).ConfigureAwait(false);
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure to clear reminders for Service {this._serviceId}.");
                throw;
            }
        }

        public async Task<string> UpsertRow(ReminderEntry entry)
        {
            try
            {
                ReminderEntity entity = ToEntity(entry);

                var response = await ExecuteWithRetries(() =>
                {
                    var ac = new AccessCondition { Condition = entry.ETag, Type = AccessConditionType.IfMatch };
                    var pk = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, entry.GrainRef));

                    return this._dbClient.UpsertDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                        entity,
                        new RequestOptions { AccessCondition = ac, PartitionKey = pk }
                    );
                }).ConfigureAwait(false);

                return response.Resource.ETag;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure to upsert reminder for Service {this._serviceId}.");
                throw;
            }
        }

        private ReminderEntity ToEntity(ReminderEntry entry)
        {
            return new ReminderEntity
            {
                Id = ReminderEntity.ConstructId(entry.GrainRef, entry.ReminderName),
                PartitionKey = ReminderEntity.ConstructPartitionKey(this._serviceId, entry.GrainRef),
                ServiceId = this._serviceId,
                GrainHash = entry.GrainRef.GetUniformHashCode(),
                GrainId = entry.GrainRef.ToKeyString(),
                Name = entry.ReminderName,
                StartAt = entry.StartAt,
                Period = entry.Period
            };
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

        private ReminderEntry FromEntity(ReminderEntity entity)
        {
            return new ReminderEntry
            {
                GrainRef = this._grainReferenceConverter.GetGrainFromKeyString(entity.GrainId),
                ReminderName = entity.Name,
                Period = entity.Period,
                StartAt = entity.StartAt.UtcDateTime,
                ETag = entity.ETag
            };
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

        private async Task TryCreateCosmosDBResources()
        {
            var dbResponse = await this._dbClient.CreateDatabaseIfNotExistsAsync(new Database { Id = this._options.DB });

            var remindersCollection = new DocumentCollection
            {
                Id = this._options.Collection
            };

            remindersCollection.PartitionKey.Paths.Add(PARTITION_KEY_PATH);

            remindersCollection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            remindersCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            remindersCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/StartAt/*" });
            remindersCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Period/*" });

            const int maxRetries = 3;
            for (var retry = 0; retry <= maxRetries; ++retry)
            {
                var collResponse = await this._dbClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(this._options.DB),
                remindersCollection,
                new RequestOptions
                {
                    PartitionKey = new PartitionKey(PARTITION_KEY_PATH),
                    ConsistencyLevel = this._options.GetConsistencyLevel(),
                    OfferThroughput =
                            this._options.CollectionThroughput >= 400
                            ? (int?)this._options.CollectionThroughput
                            : null
                });
                if (retry == maxRetries || dbResponse.StatusCode != HttpStatusCode.Created || collResponse.StatusCode == HttpStatusCode.Created)
                {
                    break;  // Apparently some throttling logic returns HttpStatusCode.OK (not 429) when the collection wasn't created in a new DB.
                }
                await Task.Delay(1000);
            }
        }
    }
}
