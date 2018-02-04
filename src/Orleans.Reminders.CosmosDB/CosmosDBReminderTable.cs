using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Reminders.CosmosDB.Models;
using Orleans.Reminders.CosmosDB.Options;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
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
        private readonly SiloOptions _siloOptions;
        private readonly AzureCosmosDBReminderProviderOptions _options;

        private DocumentClient _dbClient;

        public CosmosDBReminderTable(IGrainReferenceConverter grainReferenceConverter,
            ILoggerFactory loggerFactory,
            IOptions<SiloOptions> siloOptions,
            IOptions<AzureCosmosDBReminderProviderOptions> options)
        {
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory.CreateLogger(nameof(CosmosDBReminderTable));
            this._siloOptions = siloOptions.Value;
            this._options = options.Value;
            this._grainReferenceConverter = grainReferenceConverter;

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

        public async Task Init(GlobalConfiguration config)
        {
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

        public async Task<ReminderEntry> ReadRow(GrainReference grainRef, string reminderName)
        {
            try
            {
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<ReminderEntity>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_ROW_SPROC),
                       this._siloOptions.ServiceId, grainRef.ToKeyString(), reminderName)).ConfigureAwait(false);

                if (spResponse.Response == null) return null;

                return this.FromEntity(spResponse.Response);
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading reminder {reminderName} for service {this._siloOptions.ServiceId} and grain {grainRef.ToKeyString()}.");
                throw;
            }
        }

        public async Task<ReminderTableData> ReadRows(GrainReference key)
        {
            try
            {
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<List<ReminderEntity>>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_ROWS_SPROC),
                       this._siloOptions.ServiceId, key.ToKeyString())).ConfigureAwait(false);

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
                       this._siloOptions.ServiceId, begin, end)).ConfigureAwait(false);

                return new ReminderTableData(spResponse.Response.Select(this.FromEntity));
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure reading reminders for Service {this._siloOptions.ServiceId} for range {begin} to {end}.");
                throw;
            }
        }

        public async Task<bool> RemoveRow(GrainReference grainRef, string reminderName, string eTag)
        {
            try
            {
                var spResponse = await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<bool>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, DELETE_ROW_SPROC),
                       this._siloOptions.ServiceId, grainRef.ToKeyString(), reminderName, eTag)).ConfigureAwait(false);

                return spResponse.Response;
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure removing reminders for Service {this._siloOptions.ServiceId} with grainId {grainRef.ToKeyString()} and name {reminderName}.");
                throw;
            }
        }

        public async Task TestOnlyClearTable()
        {
            try
            {
                await ExecuteWithRetries(() => this._dbClient.ExecuteStoredProcedureAsync<bool>(
                       UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, DELETE_ROWS_SPROC),
                       this._siloOptions.ServiceId)).ConfigureAwait(false);
            }
            catch (Exception exc)
            {
                this._logger.LogError(exc, $"Failure to clear reminders for Service {this._siloOptions.ServiceId}.");
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
                this._logger.LogError(exc, $"Failure to upsert reminder for Service {this._siloOptions.ServiceId}.");
                throw;
            }
        }

        private ReminderEntity ToEntity(ReminderEntry entry)
        {
            return new ReminderEntity
            {
                ServiceId = this._siloOptions.ServiceId,
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

        private async Task TryCreateCosmosDBResources()
        {
            await this._dbClient.CreateDatabaseIfNotExistsAsync(new Database { Id = this._options.DB });

            var clusterCollection = new DocumentCollection
            {
                Id = this._options.Collection
            };

            // TODO: Set indexing policy to the collection

            await this._dbClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(this._options.DB),
                clusterCollection,
                new RequestOptions
                {
                    //TODO: Check the consistency level for the emulator
                    //ConsistencyLevel = ConsistencyLevel.Strong,
                    OfferThroughput = this._options.CollectionThroughput
                });
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
