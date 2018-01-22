function ReadSiloEntity(clusterId, siloId) {
  var context = getContext();
  var collection = context.getCollection();
  var response = context.getResponse();

  var query = 'SELECT * FROM c WHERE c.ClusterId = "' + clusterId + '"';
  var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
    function (err, docs, responseOptions) {
      if (err) throw new Error("Error: " + err.message);

      if (docs.length > 0) {
        var clusterVersion = docs.find(d => d.EntityType === "ClusterVersionEntity");
        var silos = docs.filter(d => d.EntityType === "SiloEntity" && d.id === siloId);
        response.setBody({ ClusterVersion: clusterVersion, Silos: silos });
      } else {
        response.setBody({ ClusterVersion: null, Silos: [] });
      }
    });
}
