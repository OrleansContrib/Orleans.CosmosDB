using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Providers;

namespace Orleans.CosmosDB.Tests.Grains
{
    public interface ITestCustomPartitionGrain : IGrainWithGuidKey
    {
        Task Write(string text);
        Task<List<string>> Read();

        Task Deactivate();

        Task ClearState();
    }


    [StorageProvider(ProviderName = OrleansFixture.TEST_STORAGE)]
    public class TestCustomPartitionGrain : Grain<TestState>, ITestCustomPartitionGrain
    {
        public Task<List<string>> Read()
        {
            return Task.FromResult(this.State.Data);
        }

        public Task Write(string text)
        {
            this.State.Data.Add(text);
            return WriteStateAsync();
        }

        public Task Deactivate()
        {
            this.DeactivateOnIdle();
            return Task.CompletedTask;
        }

        public Task ClearState() => this.ClearStateAsync();
    }
}