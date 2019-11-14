using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
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
        private DocumentClient _dbClient;

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
            try
            {
                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<List<SiloEntity>>(
                    UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, SPROC),
                    new RequestOptions { PartitionKey = new PartitionKey(this._clusterId) },
                    this._clusterId);

                var uris = spResponse.Response.Select(ConvertToGatewayUri).ToList();
                return uris;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task InitializeGatewayListProvider()
        {
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
        }

        private static Uri ConvertToGatewayUri(SiloEntity gateway)
        {
            SiloAddress address = SiloAddress.New(new IPEndPoint(IPAddress.Parse(gateway.Address), gateway.ProxyPort.Value), gateway.Generation);
            return address.ToGatewayUri();
        }
    }
}
