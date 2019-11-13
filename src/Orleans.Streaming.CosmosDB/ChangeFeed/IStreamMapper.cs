using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;

namespace Orleans.Streaming.CosmosDB
{
    /// <summary>
    /// Types must implement this interface to map a CosmosDB Document to a particular stream.
    /// Custom implementations of this interface can use any arbitrary logic to define the stream identity based on the the document.
    /// </summary>
    public interface IStreamMapper
    {
        /// <summary>
        /// Map a document to a stream Identity
        /// </summary>
        /// <param name="document">The document</param>
        /// <returns>Tuple containing the Stream Identity (StreamId, StreamNamespace)</returns>
        ValueTask<(Guid StreamId, string StreamNamespace)> GetStreamIdentity(Document document);
    }
}