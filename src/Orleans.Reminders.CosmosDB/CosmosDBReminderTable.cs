using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Reminders.CosmosDB.Models;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;

namespace Orleans.Reminders.CosmosDB
{
    internal class CosmosDBReminderTable : IReminderTable
    {
        private const string READ_RANGE_ROW_SPROC = "ReadRangeRows";
        private const string READ_ROW_SPROC = "ReadRow";
        private const string READ_ROWS_SPROC = "ReadRows";
        private const string DELETE_ROW_SPROC = "DeleteRow";
        private const string UPSERT_ROW_SPROC = "UpsertRow";
        private const string DELETE_ROWS_SPROC = "DeleteRows";

        private readonly Dictionary<string, string> _sprocFiles;
        private readonly IGrainReferenceConverter _grainReferenceConverter;
        private readonly ILogger _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly CosmosDBReminderStorageOptions _options;
        private readonly Guid _serviceId;

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
            this._serviceId = string.IsNullOrWhiteSpace(_clusterOptions.Value.ServiceId) ? Guid.Empty : Guid.Parse(_clusterOptions.Value.ServiceId);

            this._sprocFiles = new Dictionary<string, string>
            {
                { READ_RANGE_ROW_SPROC, $"{READ_RANGE_ROW_SPROC}.js" },
                { READ_ROW_SPROC, $"{READ_ROW_SPROC}.js" },
                { READ_ROWS_SPROC, $"{READ_ROWS_SPROC}.js" },
                { DELETE_ROW_SPROC, $"{DELETE_ROW_SPROC}.js" },
                { UPSERT_ROW_SPROC, $"{UPSERT_ROW_SPROC}.js" },
                { DELETE_ROWS_SPROC, $"{DELETE_ROWS_SPROC}.js" }
            };
        }

        public async Task Init()
        {
            var stopWatch = Stopwatch.StartNew();

            try
            {
                var initMsg = string.Format("Init: Name={0} ServiceId={1} Collection={2}",
                        nameof(CosmosDBReminderTable), this._serviceId, this._options.Collection);

                this._logger.LogInformation($"Azure Cosmos DB Reminder Storage {nameof(CosmosDBReminderTable)} is initializing: {initMsg}");

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
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<ReminderEntity>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_ROW_SPROC),
                       this._serviceId, grainRef.ToKeyString(), reminderName)).ConfigureAwait(false);

                if (spResponse.Response == null) return null;

                return this.FromEntity(spResponse.Response);
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading reminder {reminderName} for service {this._serviceId} and grain {grainRef.ToKeyString()}.");
                throw;
            }
        }

        public async Task<ReminderTableData> ReadRows(GrainReference key)
        {
            try
            {
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<List<ReminderEntity>>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_ROWS_SPROC),
                       this._serviceId, key.ToKeyString())).ConfigureAwait(false);

                return new ReminderTableData(spResponse.Response.Select(this.FromEntity));
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading reminders for Grain Type {key.InterfaceName} with Id {key.ToKeyString()}.");
                throw;
            }
        }

        public async Task<ReminderTableData> ReadRows(uint begin, uint end)
        {
            try
            {
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<List<ReminderEntity>>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_RANGE_ROW_SPROC),
                       this._serviceId, begin, end)).ConfigureAwait(false);

                return new ReminderTableData(spResponse.Response.Select(this.FromEntity));
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
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<bool>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, DELETE_ROW_SPROC),
                       this._serviceId, grainRef.ToKeyString(), reminderName, eTag)).ConfigureAwait(false);

                return spResponse.Response;
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
                await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<bool>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, DELETE_ROWS_SPROC),
                       this._serviceId)).ConfigureAwait(false);
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
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<string>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, UPSERT_ROW_SPROC),
                       entity)).ConfigureAwait(false);

                return spResponse.Response;
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
                    ConsistencyLevel = this._options.GetConsistencyLevel(),
                    OfferThroughput = this._options.CollectionThroughput
                });
                if (retry == maxRetries || dbResponse.StatusCode != HttpStatusCode.Created || collResponse.StatusCode == HttpStatusCode.Created)
                {
                    break;  // Apparently some throttling logic returns HttpStatusCode.OK (not 429) when the collection wasn't created in a new DB.
                }
                await Task.Delay(1000);
            }
        }

        private async Task UpdateStoredProcedures()
        {
            var assembly = Assembly.GetExecutingAssembly();
            foreach (var sproc in this._sprocFiles.Keys)
            {
                using (var fileStream = assembly.GetManifestResourceStream($"Orleans.Reminders.CosmosDB.Sprocs.{this._sprocFiles[sproc]}"))
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
            catch (DocumentClientException dce) when (dce.StatusCode == HttpStatusCode.NotFound)
            {
                insertStoredProc = true;
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
