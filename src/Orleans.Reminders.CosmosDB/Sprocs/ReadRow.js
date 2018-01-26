function ReadRow(serviceId, grainId, name) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  if (!serviceId) throw new Error('serviceId is required');
  if (!grainId) throw new Error('grainId is required');
  if (!name) throw new Error('name is required');

  var query = 'SELECT * FROM c WHERE c.ServiceId = "' + serviceId +
    '" AND c.GrainId = "' + grainId + '" AND c.Name = "' + name + '"';

  var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
    function (err, docs, responseOptions) {
      if (err) throw new Error("Error: " + err.message);

      if (docs.length === 0) {
        response.setBody(null);
      } else {
        response.setBody(docs[0]);
      }
    });
}
