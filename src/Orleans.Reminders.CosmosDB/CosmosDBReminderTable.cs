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
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace Orleans.Reminders.CosmosDB
{
    internal class CosmosDBReminderTable : IReminderTable
    {
        private const string READ_ROWS_QUERY = "SELECT * FROM c WHERE c.ServiceId = @serviceId";
        private const string READ_ROWS_BEGIN_LESS_THAN_END = " AND c.GrainHash > @begin AND c.GrainHash <= @end";
        private const string READ_ROWS_BEGIN_GREATER_THAN_END = " AND ((c.GrainHash > @begin) OR (c.GrainHash <= @end))";
        private const string PARTITION_KEY_PATH = "/PartitionKey";
        private readonly IGrainReferenceConverter _grainReferenceConverter;
        private readonly ILogger _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly CosmosDBReminderStorageOptions _options;
        private readonly string _serviceId;

        private CosmosClient _cosmos;
        private Container _container;

        private const HttpStatusCode TooManyRequests = (HttpStatusCode)429;

        public CosmosDBReminderTable(
            IGrainReferenceConverter grainReferenceConverter,
            ILoggerFactory loggerFactory,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CosmosDBReminderStorageOptions> options)
        {
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory.CreateLogger(nameof(CosmosDBReminderTable));
            this._options = options.Value;
            this._grainReferenceConverter = grainReferenceConverter;
            this._serviceId = string.IsNullOrWhiteSpace(clusterOptions.Value.ServiceId) ? Guid.Empty.ToString() : clusterOptions.Value.ServiceId;
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
                    this._cosmos = this._options.Client;
                }
                else
                {
                    this._cosmos = new CosmosClient(this._options.AccountEndpoint, this._options.AccountKey,
                    new CosmosClientOptions
                    {
                        ConnectionMode = this._options.ConnectionMode
                    });
                }

                this._container = this._cosmos.GetContainer(this._options.DB, this._options.Collection);

                if (this._options.CanCreateResources)
                {
                    if (this._options.DropDatabaseOnInit)
                    {
                        await this.TryDeleteDatabase();
                    }

                    await this.TryCreateCosmosDBResources();
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

                    ItemResponse<ReminderEntity> response = null;
                    try
                    {
                        response = await this._container.ReadItemAsync<ReminderEntity>(
                            ReminderEntity.ConstructId(grainRef, reminderName), pk);
                    }
                    catch (CosmosException ce) when (ce.StatusCode == HttpStatusCode.NotFound)
                    {
                        return null;
                    }

                    return response.Resource;
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

                    var query = this._container.GetItemLinqQueryable<ReminderEntity>(
                        requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, grainRef)) }
                    ).ToFeedIterator();

                    var reminders = new List<ReminderEntity>();
                    string continuation = string.Empty;
                    do
                    {
                        var queryResponse = await query.ReadNextAsync();
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
                    var sql = new StringBuilder(READ_ROWS_QUERY);

                    if (begin < end)
                    {
                        sql.Append(READ_ROWS_BEGIN_LESS_THAN_END);
                    }
                    else
                    {
                        sql.Append(READ_ROWS_BEGIN_GREATER_THAN_END);
                    }

                    var sqlSpec = new QueryDefinition(sql.ToString())
                        .WithParameter("@serviceId", this._serviceId)
                        .WithParameter("@begin", begin)
                        .WithParameter("@end", end);

                    var query = this._container.GetItemQueryIterator<ReminderEntity>(
                        sqlSpec
                    );

                    var reminders = new List<ReminderEntity>();

                    do
                    {
                        var queryResponse = await query.ReadNextAsync().ConfigureAwait(false);
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
                    var pk = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, grainRef));

                    return this._container.DeleteItemAsync<ReminderEntity>(
                        ReminderEntity.ConstructId(grainRef, reminderName),
                        pk,
                        new ItemRequestOptions { IfMatchEtag = eTag }
                    );
                }).ConfigureAwait(false);

                return true;
            }
            catch (CosmosException dce) when (dce.StatusCode == HttpStatusCode.PreconditionFailed)
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
                    var query = this._container.GetItemLinqQueryable<ReminderEntity>().ToFeedIterator();

                    var reminders = new List<ReminderEntity>();
                    do
                    {
                        var queryResponse = await query.ReadNextAsync().ConfigureAwait(false);
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
                        return this._container.DeleteItemAsync<ReminderEntity>(entity.Id, new PartitionKey(entity.PartitionKey));
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
                ReminderEntity entity = this.ToEntity(entry);

                var response = await ExecuteWithRetries(() =>
                {
                    var pk = new PartitionKey(ReminderEntity.ConstructPartitionKey(this._serviceId, entry.GrainRef));

                    return this._container.UpsertItemAsync(
                        entity,
                        pk,
                        new ItemRequestOptions { IfMatchEtag = entry.ETag }
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
                catch (CosmosException dce) when (dce.StatusCode == TooManyRequests)
                {
                    sleepTime = dce.RetryAfter ?? dce.RetryAfter.Value;
                }
                catch (AggregateException ae) when ((ae.InnerException is CosmosException dce) && dce.StatusCode == TooManyRequests)
                {
                    sleepTime = dce.RetryAfter ?? dce.RetryAfter.Value;
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
                await this._cosmos.GetDatabase(this._options.DB).DeleteAsync();
            }
            catch (CosmosException dce) when (dce.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return;
            }
        }

        private async Task TryCreateCosmosDBResources()
        {
            var offerThroughput =
                    this._options.CollectionThroughput >= 400
                    ? (int?)this._options.CollectionThroughput
                    : null;

            var dbResponse = await this._cosmos.CreateDatabaseIfNotExistsAsync(this._options.DB, offerThroughput);
            var db = dbResponse.Database;

            var remindersCollection = new ContainerProperties(this._options.Collection, PARTITION_KEY_PATH);

            remindersCollection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            remindersCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            remindersCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/StartAt/*" });
            remindersCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Period/*" });
            remindersCollection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;

            const int maxRetries = 3;
            for (var retry = 0; retry <= maxRetries; ++retry)
            {
                var collResponse = await db.CreateContainerIfNotExistsAsync(
                   remindersCollection, offerThroughput);

                if (retry == maxRetries || dbResponse.StatusCode != HttpStatusCode.Created || collResponse.StatusCode == HttpStatusCode.Created)
                {
                    break;  // Apparently some throttling logic returns HttpStatusCode.OK (not 429) when the collection wasn't created in a new DB.
                }
                await Task.Delay(1000);
            }
        }
    }
}
