function ReadState(grainType, id) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  var query = 'SELECT * FROM c WHERE c.GrainType = "' + grainType + '" AND id = "' + id + '"';
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
