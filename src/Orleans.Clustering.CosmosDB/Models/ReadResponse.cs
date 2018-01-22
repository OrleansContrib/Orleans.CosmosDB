using System.Collections.Generic;

namespace Orleans.Clustering.CosmosDB.Models
{
    internal class ReadResponse
    {
        public ClusterVersionEntity ClusterVersion { get; set; }
        public List<SiloEntity> Silos { get; set; }
    }
}
