using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Concurrency;
using Orleans.Streams;

namespace Orleans.Streaming.CosmosDB
{
    internal interface IChangeFeedDispatcher : IGrainWithStringKey
    {
        Task Dispatch(Immutable<IReadOnlyList<(Guid StreamId, string StreamNamespace, Document Document)>> batch);
    }

    /// <summary>
    /// This grain dispatch messages to the subscribers.
    /// This stateless worker poll is identified by {ProviderName}-{PartitionRagenId}.
    /// That way, we can scale better the dispatching process based on the ranges a given processor have.
    /// This grain exist to close the gap on GrainServices feature that doesn't allow it to directly talk to Streams.
    /// </summary>
    [StatelessWorker]
    internal class ChangeFeedDispatcher : Grain, IChangeFeedDispatcher
    {
        private const char KEY_SEPARATOR = '-';
        private readonly ILogger _logger;
        private readonly TimeSpan _latencyAnnouncementWindow;
        private IStreamProvider _streamProvider;
        private DateTime? _lastLatencyOutput;

        public ChangeFeedDispatcher(ILoggerFactory loggerFactory, IOptions<CosmosDBStreamOptions> options)
        {
            this._logger = loggerFactory.CreateLogger<ChangeFeedDispatcher>();
            this._latencyAnnouncementWindow = options.Value.MaxLatencyThreshold;
        }

        public override Task OnActivateAsync()
        {
            // The stream provider is registered by a name, and the first part of this stateless worker poll key, is that name.
            this._streamProvider = this.GetStreamProvider(this.GetPrimaryKeyString().Split(KEY_SEPARATOR)[0]);
            return Task.CompletedTask;
        }

        public async Task Dispatch(Immutable<IReadOnlyList<(Guid StreamId, string StreamNamespace, Document Document)>> batch)
        {
            var consumeLatency = DateTime.UtcNow - batch.Value.First().Document.Timestamp;

            if (_lastLatencyOutput is null || DateTime.UtcNow - _lastLatencyOutput >= _latencyAnnouncementWindow)
            {
                if (consumeLatency > _latencyAnnouncementWindow)
                {
                    _logger.LogWarning($"Cosmos consume latency estimated at '{consumeLatency}' exceeded the maximum allowed of '{_latencyAnnouncementWindow}'.");
                    _lastLatencyOutput = DateTime.UtcNow;
                }
                this._logger.LogInformation($"Cosmos consume latency estimated at '{consumeLatency}'.");
            }

            foreach (var change in batch.Value)
            {
                if (change.StreamId == Guid.Empty ||
                    string.IsNullOrWhiteSpace(change.StreamNamespace))
                {
                    this._logger.LogWarning($"Invalid stream identity. The document will be skipped: '{JsonConvert.SerializeObject(change.Document)}'");
                    continue;
                }

                var stream = this._streamProvider.GetStream<Document>(change.StreamId, change.StreamNamespace);
                try
                {
                    await stream.OnNextAsync(change.Document);
                }
                catch (Exception exc)
                {
                    this._logger.LogError(exc, $"Failure dispatching message from change feed. DocumentId: {change.Document.Id} | Error: {exc.Message}.");
                }

                if (this._logger.IsEnabled(LogLevel.Trace))
                {
                    this._logger.LogWarning($"Dispatched document: {JsonConvert.SerializeObject(change)}");
                }
            }
        }
    }
}