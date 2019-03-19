using Orleans.Runtime;
using System.Threading.Tasks;

namespace Orleans.Persistence.CosmosDB.Options
{
    public interface IPartitionKeyProvider
    {
        ValueTask<string> GetPartitionKey(string grainType, GrainReference grainReference);
    }

    internal class DefaultPartitionKeyProvider : IPartitionKeyProvider
    {
        public ValueTask<string> GetPartitionKey(string grainType, GrainReference grainReference) 
        {
            return new ValueTask<string>(grainType);
        }
    }
}
