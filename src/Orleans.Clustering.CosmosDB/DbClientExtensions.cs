using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Orleans.Clustering.CosmosDB
{
    public static class DbClientExtensions
    {
        public static async Task<List<T>> Query<T>(this IDocumentClient dbClient, string database, string collection, Expression<Func<T, bool>> predicate, int limit = -1)
        {
            var query = dbClient.CreateDocumentQuery<T>(
               UriFactory.CreateDocumentCollectionUri(database, collection),
               new FeedOptions { MaxItemCount = -1 })
               .Where(predicate);

            var results = new List<T>();
            var docQuery = query.AsDocumentQuery();
            while (docQuery.HasMoreResults)
            {
                results.AddRange(await docQuery.ExecuteNextAsync<T>());
            }

            return results;
        }
    }
}
