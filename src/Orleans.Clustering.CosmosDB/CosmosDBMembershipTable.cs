using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB.Models;
using Orleans.Clustering.CosmosDB.Options;
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
        private const string CLUSTER_VERSION_ID = "ClusterVersion";
        private const string PARTITION_KEY = "/ClusterId";
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly AzureCosmosDBClusteringOptions _options;
        private DocumentClient _dbClient;

        public CosmosDBMembershipTable(ILoggerFactory loggerFactory, IOptions<AzureCosmosDBClusteringOptions> clusteringOptions)
        {
            this._loggerFactory = loggerFactory;
            this._logger = loggerFactory?.CreateLogger<CosmosDBMembershipTable>();
            this._options = clusteringOptions.Value;
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
                await TryCreateCosmosDBResources();
            }

            var versionEntity = (await this._dbClient.Query<ClusterVersionEntity>(
                this._options.DB, this._options.Collection, t =>
                    t.ClusterId == this._options.ClusterId &&
                    t.EntityType == nameof(ClusterVersionEntity))).SingleOrDefault();

            if (versionEntity == null)
            {
                versionEntity = new ClusterVersionEntity
                {
                    ClusterId = this._options.ClusterId,
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
                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<ReadResponse>(
                    UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "ReadAll"),
                    new RequestOptions { PartitionKey = new PartitionKey(this._options.ClusterId) },
                    this._options.ClusterId);

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
                        //TODO: Log it
                        throw;
                    }
                }

                var data = new MembershipTableData(memEntries, version);
                return data;
            }
            catch (Exception exc)
            {
                this._logger.LogWarning($"Failure reading all silo entries for cluster id {this._options.ClusterId}: {exc}");
                throw;
            }
        }

        public Task DeleteMembershipTableEntries(string clusterId)
        {
            return this._dbClient.ExecuteStoredProcedureAsync<int>(
                UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "DeleteAllEntries"),
                        new RequestOptions { PartitionKey = new PartitionKey(clusterId) },
                        clusterId);
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                var siloEntity = ConvertToEntity(entry, this._options.ClusterId);
                var versionEntity = BuildVersionEntity(tableVersion);

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<bool>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "InsertSiloEntity"),
                        new RequestOptions { PartitionKey = new PartitionKey(this._options.ClusterId) },
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
                    UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "ReadSiloEntity"),
                    new RequestOptions { PartitionKey = new PartitionKey(this._options.ClusterId) },
                    this._options.ClusterId, id);

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
                        //TODO: Log it
                        throw;
                    }
                }

                var data = new MembershipTableData(memEntries, version);
                return data;
            }
            catch (Exception exc)
            {
                this._logger.LogWarning($"Failure reading silo entry {id} for cluster id {this._options.ClusterId}: {exc}");
                throw;
            }

        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            try
            {
                var siloEntity = ConvertToEntity(entry, this._options.ClusterId);

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<bool>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "UpdateIAmAlive"),
                        new RequestOptions { PartitionKey = new PartitionKey(this._options.ClusterId) },
                        siloEntity.Id, siloEntity.IAmAliveTime);

                if(!spResponse.Response) throw new InvalidOperationException("Unable to update IAmAlive");
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
                var siloEntity = ConvertToEntity(entry, this._options.ClusterId);
                siloEntity.ETag = etag;

                var versionEntity = BuildVersionEntity(tableVersion);

                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<bool>(
                        UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "UpdateSiloEntity"),
                        new RequestOptions { PartitionKey = new PartitionKey(this._options.ClusterId) },
                        siloEntity, versionEntity);

                return spResponse.Response;
            }
            catch (DocumentClientException exc)
            {
                if (exc.StatusCode == HttpStatusCode.PreconditionFailed) return false;
                throw;
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
            // TODO: Set indexing policy to the collection

            await this._dbClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(this._options.DB),
                clusterCollection,
                new RequestOptions
                {
                    PartitionKey = new PartitionKey(PARTITION_KEY),
                    //TODO: Check the consistency level for the emulator
                    //ConsistencyLevel = ConsistencyLevel.Strong,
                    OfferThroughput = this._options.CollectionThroughout
                });
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
                ClusterId = this._options.ClusterId,
                ClusterVersion = tableVersion.Version,
                Id = CLUSTER_VERSION_ID,
                ETag = tableVersion.VersionEtag
            };
        }
    }
}
