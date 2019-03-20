using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB.Models;
using Orleans.Configuration;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;

namespace Orleans.Clustering.CosmosDB
{
    internal class CosmosDBMembershipTable : IMembershipTable
    {
        private const string DELETE_ALL_SPROC = "DeleteAllEntries";
        private const string GET_ALIVE_GATEWAYS_SPROC = "GetAliveGateways";
        private const string INSERT_SILO_SPROC = "InsertSiloEntity";
        private const string READ_ALL_SPROC = "ReadAll";
        private const string READ_SILO_SPROC = "ReadSiloEntity";
        private const string UPDATE_I_AM_ALIVE_SPROC = "UpdateIAmAlive";
        private const string UPDATE_SILO_SPROC = "UpdateSiloEntity";
        private const string CLUSTER_VERSION_ID = "ClusterVersion";
        private const string PARTITION_KEY = "/ClusterId";

        private readonly Dictionary<string, string> _sprocFiles;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly CosmosDBClusteringOptions _options;
        private readonly ClusterOptions _clusterOptions;

        private DocumentClient _dbClient;

        public CosmosDBMembershipTable(ILoggerFactory loggerFactory, IOptions<ClusterOptions> clusterOptions, IOptions<CosmosDBClusteringOptions> clusteringOptions)
        {
            this._clusterOptions = clusterOptions.Value;
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory?.CreateLogger<CosmosDBMembershipTable>();
            this._options = clusteringOptions.Value;
            this._sprocFiles = new Dictionary<string, string>
            {
                { DELETE_ALL_SPROC, $"{DELETE_ALL_SPROC}.js" },
                { GET_ALIVE_GATEWAYS_SPROC, $"{GET_ALIVE_GATEWAYS_SPROC}.js" },
                { INSERT_SILO_SPROC, $"{INSERT_SILO_SPROC}.js" },
                { READ_ALL_SPROC, $"{READ_ALL_SPROC}.js" },
                { READ_SILO_SPROC, $"{READ_SILO_SPROC}.js" },
                { UPDATE_I_AM_ALIVE_SPROC, $"{UPDATE_I_AM_ALIVE_SPROC}.js" },
                { UPDATE_SILO_SPROC, $"{UPDATE_SILO_SPROC}.js" },
            };
        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
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

            var versionEntity = (await this._dbClient.Query<ClusterVersionEntity>(
                this._options.DB, this._options.Collection, t =>
                    t.ClusterId == this._clusterOptions.ClusterId &&
                    t.EntityType == nameof(ClusterVersionEntity))).SingleOrDefault();

            if (versionEntity == null)
            {
                versionEntity = new ClusterVersionEntity
                {
                    ClusterId = this._clusterOptions.ClusterId,
                    ClusterVersion = 0,
                    Id = CLUSTER_VERSION_ID
                };

                var response = await this._dbClient.CreateDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(this._options.DB, this._options.Collection),
                    versionEntity,
                    new RequestOptions
                    {
                        PartitionKey = new PartitionKey(versionEntity.ClusterId)
                    });

                if (response.StatusCode == HttpStatusCode.Created)
                    this._logger?.Info("Created new Cluster Version entity.");
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
            try
            {
                ReadResponse spResponse = await ReadRecords();

                ClusterVersionEntity versionEntity = spResponse.ClusterVersion;
                List<SiloEntity> entryEntities = spResponse.Silos;

                TableVersion version = null;
                if (versionEntity != null)
                {
                    version = new TableVersion(versionEntity.ClusterVersion, versionEntity.ETag);
                }
                else
                {
                    this._logger.LogError("Initial ClusterVersionEntity entity doesn't exist.");
                }

                var memEntries = new List<Tuple<MembershipEntry, string>>();
                foreach (var entity in entryEntities)
                {
                    try
                    {
                        MembershipEntry membershipEntry = ParseEntity(entity);
                        memEntries.Add(new Tuple<MembershipEntry, string>(membershipEntry, entity.ETag));
                    }
                    catch (Exception exc)
                    {
                        this._logger.LogError(exc, "Failure reading all membership records.");
                        throw;
                    }
                }

                var data = new MembershipTableData(memEntries, version);
                return data;
            }
            catch (Exception exc)
            {
                this._logger.LogWarning($"Failure reading all silo entries for cluster id {this._clusterOptions.ClusterId}: {exc}");
                throw;
            }
        }

        public Task DeleteMembershipTableEntries(string clusterId)
        {
            return this._dbClient.ExecuteStoredProcedureAsync<int>(
                UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, DELETE_ALL_SPROC),
                        new RequestOptions { PartitionKey = new PartitionKey(clusterId) },
                        clusterId);
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                var siloEntity = ConvertToEntity(entry, this._clusterOptions.ClusterId);
                var versionEntity = BuildVersionEntity(tableVersion);

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<bool>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, INSERT_SILO_SPROC),
                        new RequestOptions { PartitionKey = new PartitionKey(this._clusterOptions.ClusterId) },
                        siloEntity, versionEntity);

                return spResponse.Response;
            }
            catch (DocumentClientException exc)
            {
                if (exc.StatusCode == HttpStatusCode.PreconditionFailed) return false;
                throw;
            }
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            var id = ConstructSiloEntityId(key);

            try
            {
                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<ReadResponse>(
                    UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_SILO_SPROC),
                    new RequestOptions { PartitionKey = new PartitionKey(this._clusterOptions.ClusterId) },
                    this._clusterOptions.ClusterId, id);

                ClusterVersionEntity versionEntity = spResponse.Response.ClusterVersion;
                List<SiloEntity> entryEntities = spResponse.Response.Silos;

                TableVersion version = null;
                if (versionEntity != null)
                {
                    version = new TableVersion(versionEntity.ClusterVersion, versionEntity.ETag);
                }
                else
                {
                    this._logger.LogError("Initial ClusterVersionEntity entity doesn't exist.");
                }

                var memEntries = new List<Tuple<MembershipEntry, string>>();
                foreach (var entity in entryEntities)
                {
                    try
                    {
                        MembershipEntry membershipEntry = ParseEntity(entity);
                        memEntries.Add(new Tuple<MembershipEntry, string>(membershipEntry, entity.ETag));
                    }
                    catch (Exception exc)
                    {
                        this._logger.LogError(exc, "Failure reading membership row.");
                        throw;
                    }
                }

                var data = new MembershipTableData(memEntries, version);
                return data;
            }
            catch (Exception exc)
            {
                this._logger.LogWarning($"Failure reading silo entry {id} for cluster id {this._clusterOptions.ClusterId}: {exc}");
                throw;
            }

        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            try
            {
                var siloEntity = ConvertToEntity(entry, this._clusterOptions.ClusterId);

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<bool>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, UPDATE_I_AM_ALIVE_SPROC),
                        new RequestOptions { PartitionKey = new PartitionKey(this._clusterOptions.ClusterId) },
                        siloEntity.Id, siloEntity.IAmAliveTime);

                if (!spResponse.Response) throw new InvalidOperationException("Unable to update IAmAlive");
            }
            catch (DocumentClientException)
            {
                throw;
            }
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            try
            {
                var siloEntity = ConvertToEntity(entry, this._clusterOptions.ClusterId);
                siloEntity.ETag = etag;

                var versionEntity = BuildVersionEntity(tableVersion);

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<bool>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, UPDATE_SILO_SPROC),
                        new RequestOptions { PartitionKey = new PartitionKey(this._clusterOptions.ClusterId) },
                        siloEntity, versionEntity);

                return spResponse.Response;
            }
            catch (DocumentClientException exc)
            {
                if (exc.StatusCode == HttpStatusCode.PreconditionFailed) return false;
                throw;
            }
        }

        private static MembershipEntry ParseEntity(SiloEntity entity)
        {
            var entry = new MembershipEntry
            {
                HostName = entity.Hostname,
                Status = entity.Status
            };

            if (entity.ProxyPort.HasValue)
                entry.ProxyPort = entity.ProxyPort.Value;

            entry.SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Parse(entity.Address), entity.Port), entity.Generation);

            entry.SiloName = entity.SiloName;

            entry.StartTime = entity.StartTime.UtcDateTime;

            entry.IAmAliveTime = entity.IAmAliveTime.UtcDateTime;

            var suspectingSilos = new List<SiloAddress>();
            var suspectingTimes = new List<DateTime>();

            foreach (var silo in entity.SuspectingSilos)
            {
                suspectingSilos.Add(SiloAddress.FromParsableString(silo));
            }

            foreach (var time in entity.SuspectingTimes)
            {
                suspectingTimes.Add(LogFormatter.ParseDate(time));
            }

            if (suspectingSilos.Count != suspectingTimes.Count)
                throw new OrleansException($"SuspectingSilos.Length of {suspectingSilos.Count} as read from Azure table is not eqaul to SuspectingTimes.Length of {suspectingTimes.Count}");

            for (int i = 0; i < suspectingSilos.Count; i++)
                entry.AddSuspector(suspectingSilos[i], suspectingTimes[i]);

            return entry;
        }

        private static SiloEntity ConvertToEntity(MembershipEntry memEntry, string clusterId)
        {
            var tableEntry = new SiloEntity
            {
                Id = ConstructSiloEntityId(memEntry.SiloAddress),
                ClusterId = clusterId,
                Address = memEntry.SiloAddress.Endpoint.Address.ToString(),
                Port = memEntry.SiloAddress.Endpoint.Port,
                Generation = memEntry.SiloAddress.Generation,
                Hostname = memEntry.HostName,
                Status = memEntry.Status,
                ProxyPort = memEntry.ProxyPort,
                SiloName = memEntry.SiloName,
                StartTime = memEntry.StartTime,
                IAmAliveTime = memEntry.IAmAliveTime
            };

            if (memEntry.SuspectTimes != null)
            {
                foreach (var tuple in memEntry.SuspectTimes)
                {
                    tableEntry.SuspectingSilos.Add(tuple.Item1.ToParsableString());
                    tableEntry.SuspectingTimes.Add(LogFormatter.PrintDate(tuple.Item2));
                }
            }

            return tableEntry;
        }

        private static string ConstructSiloEntityId(SiloAddress silo)
        {
            return $"{silo.Endpoint.Address}-{silo.Endpoint.Port}-{silo.Generation}";
        }

        private ClusterVersionEntity BuildVersionEntity(TableVersion tableVersion)
        {
            return new ClusterVersionEntity
            {
                ClusterId = this._clusterOptions.ClusterId,
                ClusterVersion = tableVersion.Version,
                Id = CLUSTER_VERSION_ID,
                ETag = tableVersion.VersionEtag
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
            await this._dbClient.CreateDatabaseIfNotExistsAsync(new Database { Id = this._options.DB });

            var clusterCollection = new DocumentCollection
            {
                Id = this._options.Collection
            };
            clusterCollection.PartitionKey.Paths.Add(PARTITION_KEY);

            clusterCollection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            clusterCollection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Address/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Port/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Generation/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Hostname/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/SiloName/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/\"SuspectingSilos\"/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/\"SuspectingTimes\"/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/StartTime/*" });
            clusterCollection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/IAmAliveTime/*" });

            await this._dbClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(this._options.DB),
                clusterCollection,
                new RequestOptions
                {
                    PartitionKey = new PartitionKey(PARTITION_KEY),
                    ConsistencyLevel = this._options.GetConsistencyLevel(),
                    OfferThroughput = this._options.CollectionThroughput
                });
        }

        private async Task UpdateStoredProcedures()
        {
            var assembly = Assembly.GetExecutingAssembly();
            foreach (var sproc in this._sprocFiles.Keys)
            {
                using (var fileStream = assembly.GetManifestResourceStream($"Orleans.Clustering.CosmosDB.Sprocs.{this._sprocFiles[sproc]}"))
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

        public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            var allSilos = (await this.ReadRecords()).Silos;
            if (allSilos.Count == 0) return;

            var toDelete = allSilos.Where(s => s.Status == SiloStatus.Dead && s.IAmAliveTime < beforeDate);
            var tasks = new List<Task>();
            foreach (var deadSilo in toDelete)
            {
                tasks.Add(
                    this._dbClient.DeleteDocumentAsync(
                        UriFactory.CreateDocumentUri(
                            this._options.DB,
                            this._options.Collection,
                            deadSilo.Id
                        )
                    )
                );
            }

            await Task.WhenAll(tasks);
        }

        private async Task<ReadResponse> ReadRecords()
        {
            return (await this._dbClient.ExecuteStoredProcedureAsync<ReadResponse>(
                UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, READ_ALL_SPROC),
                new RequestOptions { PartitionKey = new PartitionKey(this._clusterOptions.ClusterId) },
                this._clusterOptions.ClusterId)).Response;
        }
    }
}
