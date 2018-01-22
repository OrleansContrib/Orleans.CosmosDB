function DeleteAllEntries(clusterId) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  var query = 'SELECT * FROM c WHERE c.ClusterId = "' + clusterId + '"';
  var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
    function (err, silos, responseOptions) {
      if (err) throw new Error("Error: " + err.message);

      if (silos.length > 0) {

        silos.forEach(s => {
          collection.deleteDocument(s._self, function (exc) {
            if (exc) throw new Error('Unableble to delete SiloEntity: ' + exc.message);
          });
        });

        response.setBody(silos.length);
      } else {
        response.setBody(0);
      }
    });
}
