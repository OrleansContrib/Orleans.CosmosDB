using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Clustering.CosmosDB.Models;
using Orleans.Clustering.CosmosDB.Options;
using Orleans.Messaging;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Orleans.Clustering.CosmosDB
{
    internal class CosmosDBGatewayListProvider : IGatewayListProvider
    {
        private readonly AzureCosmosDBGatewayOptions _options;
        private readonly ILoggerFactory _loggerFactory;
        private readonly TimeSpan _maxStaleness;
        private DocumentClient _dbClient;

        public TimeSpan MaxStaleness => this._maxStaleness;

        public bool IsUpdatable => true;

        public CosmosDBGatewayListProvider(ILoggerFactory loggerFactory, IOptions<AzureCosmosDBGatewayOptions> options, ClientConfiguration clientConfiguration)
        {
            this._maxStaleness = clientConfiguration.GatewayListRefreshPeriod;
            this._loggerFactory = loggerFactory;
            this._options = options.Value;
        }

        public async Task<IList<Uri>> GetGateways()
        {
            try
            {
                var spResponse = await this._dbClient.ExecuteStoredProcedureAsync<List<SiloEntity>>(
                    UriFactory.CreateStoredProcedureUri(this._options.DB, this._options.Collection, "GetAliveGateways"),
                    new RequestOptions { PartitionKey = new PartitionKey(this._options.ClusterId) },
                    this._options.ClusterId);

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
            this._dbClient = new DocumentClient(new Uri(this._options.AccountEndpoint), this._options.AccountKey,
                    new ConnectionPolicy
                    {
                        ConnectionMode = this._options.ConnectionMode,
                        ConnectionProtocol = this._options.ConnectionProtocol
                    });

            await this._dbClient.OpenAsync();
        }

        private static Uri ConvertToGatewayUri(SiloEntity gateway)
        {
            SiloAddress address = SiloAddress.New(new IPEndPoint(IPAddress.Parse(gateway.Address), gateway.ProxyPort.Value), gateway.Generation);
            return address.ToGatewayUri();
        }
    }
}
