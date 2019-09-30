using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Streaming.CosmosDB
{
    /// <summary>
    /// Default implementation of Stream Identity Mapper.
    /// This implementation uses the Document.Id as the stream ID and the configurable CosmosDBStreamOptions.DefaultStreamNamespace as the namespace.
    /// To use this mapper, the Document.Id must be set as CosmosDB default Id field, a Guid.
    /// </summary>
    internal class IdBasedStreamMapper : IStreamMapper
    {
        private readonly CosmosDBStreamOptions _options;
        private readonly ILogger _logger;

        public IdBasedStreamMapper(IOptions<CosmosDBStreamOptions> options, ILoggerFactory loggerFactory)
        {
            this._options = options.Value;
            this._logger = loggerFactory.CreateLogger<IdBasedStreamMapper>();
        }

        /// <summary>
        /// For a given document, get the Document.Id as the stream Id, and the CosmosDBStreamOptions.DefaultStreamNamespace as the namespace.
        /// </summary>
        /// <param name="document">The CosmosDB document</param>
        /// <returns>Tuple containing the Stream Identity (StreamId, StreamNamespace)</returns>
        public ValueTask<(Guid StreamId, string StreamNamespace)> GetStreamIdentity(Document document)
        {
            if (Guid.TryParse(document.Id, out Guid id))
            {
                return new ValueTask<(Guid, string)>((Guid.Parse(document.Id), this._options.DefaultStreamNamespace));
            }
            else
            {
                this._logger.LogError($"Unable to get stream id from the document. Document.Id = '{document.Id}'. It must be a Guid in order to be used by IdBasedStreamMapper. The document will be skipped.");
                return default;
            }
        }
    }
}