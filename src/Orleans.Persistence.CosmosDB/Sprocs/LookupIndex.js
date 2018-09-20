function LookupIndex(grainType, indexedField, key) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  if (!grainType) throw new Error('grainType is required');
  if (!indexedField) throw new Error('indexedField is required');
  if (!key) throw new Error('key is required');

  // 'key' must be quoted by the caller if it is not of a numeric type.
  var query = 'SELECT * FROM c WHERE c.GrainType = "' + grainType + '" AND c.State.' + indexedField + ' = ' + key;
  var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
    function (err, docs, responseOptions) {
      if (err) throw new Error("Error: " + err.message);

      if (docs.length === 0) {
        response.setBody(null);
      } else {
        response.setBody(docs);
      }
    });
}
