using Orleans.Providers;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.CosmosDB.Tests.Grains
{
    public interface ITestGrain : IGrainWithIntegerKey
    {
        Task Write(string text);
        Task<List<string>> Read();
        Task<IGrainReminder> RegisterReminder(string name);
        Task<bool> ReminderExist(string name);
        Task<bool> ReminderTicked();
        Task DismissReminder(string name);
    }

    public class TestState
    {
        public List<string> Data { get; set; } = new List<string>();
    }

    [StorageProvider(ProviderName = OrleansFixture.TEST_STORAGE)]
    public class TestGrain : Grain<TestState>, ITestGrain, IRemindable
    {
        private bool ticked = false;
        public static TimeSpan ReminderWaitTime = TimeSpan.FromMinutes(1);  // Minimum wait time allowed by Orleans

        public async Task DismissReminder(string name)
        {
            var r = await GetReminder(name);
            await UnregisterReminder(r);
        }

        public Task<List<string>> Read()
        {
            return Task.FromResult(this.State.Data);
        }

        public Task ReceiveReminder(string reminderName, TickStatus status)
        {
            this.ticked = true;
            return Task.CompletedTask;
        }

        public Task<IGrainReminder> RegisterReminder(string name)
        {
            return RegisterOrUpdateReminder(name, TimeSpan.FromSeconds(2), ReminderWaitTime);
        }

        public async Task<bool> ReminderExist(string name)
        {
            var r = await GetReminder(name);
            return r != null;
        }

        public Task<bool> ReminderTicked()
        {
            return Task.FromResult(ticked);
        }

        public Task Write(string text)
        {
            this.State.Data.Add(text);
            return WriteStateAsync();
        }
    }
}
