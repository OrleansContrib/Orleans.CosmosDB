using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB.Models;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Orleans.Clustering.CosmosDB
{
    internal class CosmosDBGatewayListProvider : IGatewayListProvider
    {
        private const string SPROC = "GetAliveGateways";
        private readonly CosmosDBGatewayOptions _options;
        private readonly ILoggerFactory _loggerFactory;
        private readonly TimeSpan _maxStaleness;
        private readonly string _clusterId;
        private CosmosClient _cosmos;
        private Container _container;

        public TimeSpan MaxStaleness => this._maxStaleness;

        public bool IsUpdatable => true;

        public CosmosDBGatewayListProvider(ILoggerFactory loggerFactory, IOptions<CosmosDBGatewayOptions> options,
            IOptions<ClusterOptions> clusterOptions, IOptions<GatewayOptions> gatewayOptions)
        {
            this._clusterId = clusterOptions.Value.ClusterId;
            this._maxStaleness = gatewayOptions.Value.GatewayListRefreshPeriod;
            this._loggerFactory = loggerFactory;
            this._options = options.Value;
        }

        public async Task<IList<Uri>> GetGateways()
        {
            var query = this._container
                    .GetItemLinqQueryable<SiloEntity>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(this._clusterId) })
                        .Where(g => g.EntityType == nameof(SiloEntity) &&
                            g.Status == SiloStatus.Active &&
                            g.ProxyPort.HasValue && g.ProxyPort.Value != 0).ToFeedIterator();

            var entities = new List<SiloEntity>();
            do
            {
                var items = await query.ReadNextAsync();
                entities.AddRange(items);
            } while (query.HasMoreResults);

            var uris = entities.Select(ConvertToGatewayUri).ToList();
            return uris;
        }

        public Task InitializeGatewayListProvider()
        {
            if (this._options.Client != null)
            {
                this._cosmos = this._options.Client;
            }
            else
            {
                this._cosmos = new CosmosClient(
                    this._options.AccountEndpoint,
                    this._options.AccountKey,
                    new CosmosClientOptions { ConnectionMode = this._options.ConnectionMode }
                );
            }
            this._container = this._cosmos.GetDatabase(this._options.DB).GetContainer(this._options.Collection);

            return Task.CompletedTask;
        }

        private static Uri ConvertToGatewayUri(SiloEntity gateway)
        {
            SiloAddress address = SiloAddress.New(new IPEndPoint(IPAddress.Parse(gateway.Address), gateway.ProxyPort.Value), gateway.Generation);
            return address.ToGatewayUri();
        }
    }
}
