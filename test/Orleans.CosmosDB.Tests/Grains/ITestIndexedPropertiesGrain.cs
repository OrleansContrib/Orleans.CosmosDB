using Orleans.Providers;
using System.Threading.Tasks;

namespace Orleans.CosmosDB.Tests.Grains
{
    // This file is similar to Orleans.Indexing structure in that it breaks out state into indexed and non-indexed
    // properties, and includes the UserState indirection to simulate Fault-Tolerant indexing storage. However, there
    // are no Orleans indexes defined, to avoid a dependency on Orleans.Indexing. It tests that creating an index for
    // the properties that we *would* index in Orleans, and looking up based on those indexes, works as expected.
    public interface ITestIndexedPropertiesGrain : IGrainWithIntegerKey
    {
        Task ReadAsync();
        Task WriteAsync();

        Task<int> GetNftIndexedIntAsync();
        Task SetNftIndexedIntAsync(int value);

        Task<string> GetFtIndexedStringAsync();
        Task SetFtIndexedStringAsync(string value);

        Task<string> GetNonIndexedStringAsync();
        Task SetNonIndexedStringAsync(string value);
    }

    [System.Serializable]
    public class UserState
    {
        public string FtIndexedString { get; set; }
    }

    public interface IIndexedProperties
    {
        int NftIndexedInt { get; set; }
        UserState UserState { get; set; }
    }

    public interface IIndexedTestState : IIndexedProperties
    {
        string NonIndexedString { get; set; }
    }

    [System.Serializable]
    public class IndexedTestState : IIndexedTestState
    {
        public int NftIndexedInt { get; set; }
        public UserState UserState { get; set; }
        public string NonIndexedString { get; set; }
    }

    [StorageProvider(ProviderName = OrleansFixture.TEST_STORAGE)]
    public class TestIndexedPropertiesGrain : Grain<IndexedTestState>, ITestIndexedPropertiesGrain
    {
        public Task ReadAsync() => base.ReadStateAsync();
        public Task WriteAsync() => base.WriteStateAsync();

        public Task<int> GetNftIndexedIntAsync() => Task.FromResult(this.NftIndexedInt);
        public Task SetNftIndexedIntAsync(int value) { this.NftIndexedInt = value; return Task.CompletedTask; }

        public Task<string> GetFtIndexedStringAsync() => Task.FromResult(this.FtIndexedString);
        public Task SetFtIndexedStringAsync(string value) { this.FtIndexedString = value; return Task.CompletedTask; }

        public Task<string> GetNonIndexedStringAsync() => Task.FromResult(this.NonIndexedString);
        public Task SetNonIndexedStringAsync(string value) { this.NonIndexedString = value; return Task.CompletedTask; }

        private UserState GetUserState()
        {
            if (this.State.UserState == null)
            {
                this.State.UserState = new UserState();
            }
            return this.State.UserState;
        }

        public int NftIndexedInt
        {
            get => this.State.NftIndexedInt;
            set => this.State.NftIndexedInt = value;
        }

        public string FtIndexedString
        {
            get => this.GetUserState().FtIndexedString;
            set => this.GetUserState().FtIndexedString = value;
        }

        public string NonIndexedString
        {
            get => this.State.NonIndexedString;
            set => this.State.NonIndexedString = value;
        }
    }
}
