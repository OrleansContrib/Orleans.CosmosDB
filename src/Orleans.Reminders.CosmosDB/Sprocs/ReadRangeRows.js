function ReadRangeRows(serviceId, beginHash, endHash) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  if (!serviceId) throw new Error('serviceId is required');
  if (!beginHash) throw new Error('beginHash is required');
  if (!endHash) throw new Error('endHash is required');

  var query = 'SELECT * FROM c WHERE c.ServiceId = "' + serviceId + '"';

  if (beginHash < endHash) {
    query = query + ' AND c.GrainHash > ' + beginHash + ' AND c.GrainHash <= ' + endHash;
  } else {
    query = query + ' AND ((c.GrainHash > ' + beginHash + ') OR (c.GrainHash <= ' + endHash +'))';
  }

  var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
    function (err, docs, responseOptions) {
      if (err) throw new Error("Error: " + err.message);

      if (docs.length === 0) {
        response.setBody([]);
      } else {
        response.setBody(docs);
      }
    });
}
