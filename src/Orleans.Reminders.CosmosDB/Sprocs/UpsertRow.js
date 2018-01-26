function WriteState(entity) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  if (!entity) throw new Error('entity is required');

  var query = 'SELECT c.id, c._self FROM c WHERE c.GrainType = "' + entity.GrainType + '" AND c.id = "' + entity.id + '"';
  var accept = collection.upsertDocument(collection.getSelfLink(), entity,
    { accessCondition: { type: 'IfMatch', condition: entity._etag } },
    function (err, doc) {
      if (err) throw new Error("Error: " + err.message);

      response.setBody(doc._etag);
    });
}
