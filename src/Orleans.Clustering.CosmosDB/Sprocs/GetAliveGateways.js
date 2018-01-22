function GetAliveGateways(clusterId) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  var query = 'SELECT * FROM c WHERE c.ClusterId = "' + clusterId +
    '" AND c.EntityType = "SiloEntity" AND c.Status = "Active" AND is_defined(c.ProxyPort) AND c.ProxyPort <> 0';
  var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
    function (err, docs, responseOptions) {
      if (err) throw new Error("Error: " + err.message);

      response.setBody(docs);
    });
}
