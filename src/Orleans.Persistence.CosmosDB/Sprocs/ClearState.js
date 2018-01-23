function ClearState(grainType, id, eTag, isDelete) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  var query = 'SELECT * FROM c WHERE c.GrainType = "' + entity.GrainType + '" AND c.id = "' + entity.id + '"';
  var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
    function (err, docs, responseOptions) {
      if (err) throw new Error("Error: " + err.message);

      if (docs.length === 0) {
        response.setBody('');
      } else {
        var entity = docs[0];

        if (isDelete) {
          var deleteAccepted = collection.deleteDocument(entity._self,
            { accessCondition: { type: 'IfMatch', condition: eTag } },
            function (err) {
              if (err) throw new Error('Error deleting StateEntity: ' + err.message);

              if (!deleteAccepted) throw new Error('Unable to deleting StateEntity.');

              response.setBody('');
            });
        } else {
          entity.State = null;
          var updateAccepted = collection.replaceDocument(entity._self, entity,
            { accessCondition: { type: 'IfMatch', condition: eTag } },
            function (err, updatedEntity) {
              if (err) throw new Error('Error clearing StateEntity: ' + err.message);

              if (!updateAccepted) throw new Error('Unable to clearing StateEntity');

              response.setBody(updatedEntity._etag);
            });
        }
      }
    });
}
