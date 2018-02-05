function UpdateIAmAlive(siloEntityId, iAmAliveTime) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  var entityExistQuery = 'SELECT * FROM c WHERE c.id = "' + siloEntityId + '"';
  var existAccepted = collection.queryDocuments(collection.getSelfLink(), entityExistQuery, {},
    function (err, found) {
      if (!existAccepted) throw new Error('Unable to query for SiloEntity');

      if (found.length > 0) {
        var siloFound = found.find(f => f.id === siloEntityId);
        siloFound.IAmAliveTime = iAmAliveTime;

        var updateIAmAliveRequestAccepted = collection.replaceDocument(siloFound._self, siloFound,
          function (err, updatedEntity) {
            if (err) throw new Error('Error Updating IAmAlive for SiloEntity: ' + err.message);

            if (!updateIAmAliveRequestAccepted) throw new Error('Unable to write ClusterVersionEntity');

            response.setBody(true);
          });
      }
      else {
        response.setBody(false);
      }
    });
}
