using Orleans.Providers;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.CosmosDB.Tests.Grains
{
    public interface ITestGrain : IGrainWithIntegerKey
    {
        Task Write(string text);
        Task<List<string>> Read();
    }

    public class TestState
    {
        public List<string> Data { get; set; } = new List<string>();
    }

    [StorageProvider(ProviderName = PersistenceTests.TEST_STORAGE)]
    public class TestGrain : Grain<TestState>, ITestGrain
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
    }
}
