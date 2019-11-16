using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB.Models;
using Orleans.Configuration;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Orleans.Clustering.CosmosDB
{
    internal class CosmosDBMembershipTable : IMembershipTable
    {
        private const string READ_ALL_QUERY = "SELECT * FROM c";
        private const string READ_ENTRY_QUERY = "SELECT * FROM c WHERE c.id = @siloId OR c.id = 'ClusterVersion'";
        private const string CLUSTER_VERSION_ID = "ClusterVersion";
        private const string PARTITION_KEY = "/ClusterId";

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly CosmosDBClusteringOptions _options;
        private readonly ClusterOptions _clusterOptions;

        private CosmosClient _cosmos;
        private Container _container;

        public CosmosDBMembershipTable(ILoggerFactory loggerFactory, IOptions<ClusterOptions> clusterOptions, IOptions<CosmosDBClusteringOptions> clusteringOptions)
        {
            this._clusterOptions = clusterOptions.Value;
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory?.CreateLogger<CosmosDBMembershipTable>();
            this._options = clusteringOptions.Value;
        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
        {
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
                    }
                );
            }
            this._container = this._cosmos.GetDatabase(this._options.DB).GetContainer(this._options.Collection);

            if (this._options.CanCreateResources)
            {
                if (this._options.DropDatabaseOnInit)
                {
                    await this.TryDeleteDatabase();
                }

                await this.TryCreateCosmosDBResources();
            }

            ClusterVersionEntity versionEntity = null;

            try
            {
                versionEntity = (await this._container.ReadItemAsync<ClusterVersionEntity>(
                    CLUSTER_VERSION_ID,
                    new PartitionKey(this._clusterOptions.ClusterId))).Resource;
            }
            catch (CosmosException ce) when (ce.StatusCode == HttpStatusCode.NotFound)
            {
                if (versionEntity == null)
                {
                    versionEntity = new ClusterVersionEntity
                    {
                        ClusterId = this._clusterOptions.ClusterId,
                        ClusterVersion = 0,
                        Id = CLUSTER_VERSION_ID
                    };

                    var response = await this._container.CreateItemAsync(
                        versionEntity,
                        new PartitionKey(versionEntity.ClusterId)
                    );

                    if (response.StatusCode == HttpStatusCode.Created)
                        this._logger?.Info("Created new Cluster Version entity.");
                }
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
            try
            {
                (ClusterVersionEntity Version, List<SiloEntity> Silos) response = await this.ReadRecords(this._clusterOptions.ClusterId);

                ClusterVersionEntity versionEntity = response.Version;
                List<SiloEntity> entryEntities = response.Silos;

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

        public async Task DeleteMembershipTableEntries(string clusterId)
        {
            var all = await this.ReadRecords(clusterId);

            var batch = this._container.CreateTransactionalBatch(new PartitionKey(clusterId));

            foreach (var silo in all.Silos)
            {
                batch = batch.DeleteItem(silo.Id);
            }

            batch = batch.DeleteItem(all.Version.Id);

            await batch.ExecuteAsync();
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                var siloEntity = ConvertToEntity(entry, this._clusterOptions.ClusterId);
                var versionEntity = this.BuildVersionEntity(tableVersion);

                var response = await this._container.CreateTransactionalBatch(new PartitionKey(this._clusterOptions.ClusterId))
                    .ReplaceItem(versionEntity.Id, versionEntity, new TransactionalBatchItemRequestOptions { IfMatchEtag = tableVersion.VersionEtag })
                    .CreateItem(siloEntity).ExecuteAsync();

                return response.IsSuccessStatusCode;
            }
            catch (CosmosException exc)
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
                var query = this._container
                    .GetItemQueryIterator<dynamic>(
                        new QueryDefinition(READ_ENTRY_QUERY).WithParameter("@siloId", id),
                        requestOptions: new QueryRequestOptions
                        {
                            PartitionKey = new PartitionKey(this._clusterOptions.ClusterId)
                        }
                    );

                var docs = await query.ReadNextAsync();
                var versionDoc = docs.Where(i => i.EntityType == nameof(ClusterVersionEntity)).SingleOrDefault();
                ClusterVersionEntity clusterVersionEntity = versionDoc != null ? ClusterVersionEntity.FromDocument(versionDoc) : default;
                var siloEntities = docs.Where(i => i.EntityType == nameof(SiloEntity)).Select<dynamic, SiloEntity>(d => SiloEntity.FromDocument(d));

                TableVersion version = null;
                if (clusterVersionEntity != null)
                {
                    version = new TableVersion(clusterVersionEntity.ClusterVersion, clusterVersionEntity.ETag);
                }
                else
                {
                    this._logger.LogError("Initial ClusterVersionEntity entity doesn't exist.");
                }

                var memEntries = new List<Tuple<MembershipEntry, string>>();
                foreach (var entity in siloEntities)
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

        public Task UpdateIAmAlive(MembershipEntry entry)
        {
            var siloEntity = ConvertToEntity(entry, this._clusterOptions.ClusterId);

            return this._container.ReplaceItemAsync(siloEntity, siloEntity.Id, new PartitionKey(this._clusterOptions.ClusterId));
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            try
            {
                var siloEntity = ConvertToEntity(entry, this._clusterOptions.ClusterId);
                siloEntity.ETag = etag;

                var versionEntity = this.BuildVersionEntity(tableVersion);

                var response = await this._container.CreateTransactionalBatch(new PartitionKey(this._clusterOptions.ClusterId))
                    .ReplaceItem(versionEntity.Id, versionEntity, new TransactionalBatchItemRequestOptions { IfMatchEtag = tableVersion.VersionEtag })
                    .ReplaceItem(siloEntity.Id, siloEntity, new TransactionalBatchItemRequestOptions { IfMatchEtag = siloEntity.ETag }).ExecuteAsync();

                return response.IsSuccessStatusCode;
            }
            catch (CosmosException exc)
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
                await this._cosmos.GetDatabase(this._options.DB).DeleteAsync();
            }
            catch (CosmosException dce) when (dce.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return;
            }
        }

        private async Task TryCreateCosmosDBResources()
        {
            var dbResponse = (await this._cosmos.CreateDatabaseIfNotExistsAsync(this._options.DB)).Database;

            var containerProperties = new ContainerProperties(this._options.Collection, PARTITION_KEY);
            containerProperties.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            containerProperties.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Address/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Port/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Generation/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/Hostname/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/SiloName/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/\"SuspectingSilos\"/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/\"SuspectingTimes\"/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/StartTime/*" });
            containerProperties.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/IAmAliveTime/*" });
            //var consistency = this._options.GetConsistencyLevel();
            //if (consistency.HasValue)
            //{
            //    containerProperties.IndexingPolicy.IndexingMode = consistency.Value;
            //}
            containerProperties.IndexingPolicy.IndexingMode = IndexingMode.Consistent;

            await dbResponse.CreateContainerIfNotExistsAsync(
                containerProperties, this._options.CollectionThroughput);
        }

        public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            var allSilos = (await this.ReadRecords(this._clusterOptions.ClusterId)).Silos;
            if (allSilos.Count == 0) return;

            var toDelete = allSilos.Where(s => s.Status == SiloStatus.Dead && s.IAmAliveTime < beforeDate);
            var tasks = new List<Task>();
            var pk = new PartitionKey(this._clusterOptions.ClusterId);
            foreach (var deadSilo in toDelete)
            {
                tasks.Add(
                    this._container.DeleteItemAsync<SiloEntity>(
                        deadSilo.Id,
                        pk
                    )
                );
            }

            await Task.WhenAll(tasks);
        }

        private async Task<(ClusterVersionEntity Version, List<SiloEntity> Silos)> ReadRecords(string clusterId)
        {
            var query = this._container
                .GetItemQueryIterator<dynamic>(
                    READ_ALL_QUERY,
                    requestOptions: new QueryRequestOptions
                    {
                        PartitionKey = new PartitionKey(clusterId)
                    }
                );

            var silos = new List<SiloEntity>();
            ClusterVersionEntity clusterVersion = null;
            do
            {
                var items = await query.ReadNextAsync();
                var version = items.Where(i => i.EntityType == nameof(ClusterVersionEntity)).SingleOrDefault();
                if (version != null)
                {
                    clusterVersion = ClusterVersionEntity.FromDocument(version);
                }

                silos.AddRange(items.Where(i => i.EntityType == nameof(SiloEntity)).Select(d => (SiloEntity)SiloEntity.FromDocument(d)));
            } while (query.HasMoreResults);

            return (clusterVersion, silos);
        }
    }
}
