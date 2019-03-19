using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Persistence.CosmosDB.Options
{
    public interface IPartitionKeyProvider
    {
        string GetPartitionKey(string grainType, GrainReference grainReference);
    }

    internal class DefaultPartitionKeyProvider : IPartitionKeyProvider
    {
        public string GetPartitionKey(string grainType, GrainReference grainReference) 
        {
            return grainType;
        }
    }
}
