using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Concurrency;

namespace Orleans.Streaming.CosmosDB
{
    internal interface IProcessor
    {
        string Name { get; }
        Task Start();
        Task Stop();
    }

    /// <summary>
    /// Wraps CosmosDB IChangeFeedProcessor instance.
    /// One of this type is created per stream registered.
    /// It is responsible for monitoring and dispatching of changes in a given CosmosDB document Collection
    /// </summary>
    internal class ChangeFeedProcessor : IProcessor, IChangeFeedObserver, IChangeFeedObserverFactory
    {
        private readonly TaskScheduler _scheduler;
        private readonly IGrainFactory _grainFactory;
        private readonly ILogger _logger;
        private readonly string _name;
        private readonly string _siloName;
        private readonly CosmosDBStreamOptions _options;
        private readonly IStreamMapper _mapper;
        private readonly CancellationTokenSource _ctsClose;
        private readonly ChangeFeedCheckpointer _checkpointer;
        private IChangeFeedProcessor _processor;
        private Task _checkpointerTask;

        public string Name => this._name;

        private ChangeFeedProcessor(string name, string siloName,
            CosmosDBStreamOptions options, IServiceProvider serviceProvider,
            TaskScheduler scheduler, IStreamMapper mapper)
        {
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            this._name = name;
            this._siloName = siloName;
            this._scheduler = scheduler;
            this._grainFactory = serviceProvider.GetRequiredService<IGrainFactory>();
            this._logger = loggerFactory.CreateLogger($"{nameof(ChangeFeedProcessor)}-{name}");
            this._mapper = mapper;
            this._options = options;
            this._ctsClose = new CancellationTokenSource();
            this._checkpointer = new ChangeFeedCheckpointer(loggerFactory, this._options);
        }

        /// <summary>
        /// Factory method to create a new IProcessor instances
        /// </summary>
        /// <param name="name">Name of the stream provider</param>
        /// <param name="siloName">This Silo name</param>
        /// <param name="options">Configuration for both the monitored and lease collections</param>
        /// <param name="serviceProvider">DI container</param>
        /// <param name="scheduler">Orleans Task Scheduler</param>
        /// <param name="mapper">The IStreamMapper implementation</param>
        /// <returns>IProcessor implementation</returns>
        public static IProcessor Create(string name, string siloName, CosmosDBStreamOptions options, IServiceProvider serviceProvider, TaskScheduler scheduler, IStreamMapper mapper)
        {
            return new ChangeFeedProcessor(name, siloName, options, serviceProvider, scheduler, mapper);
        }

        /// <summary>
        /// Invoked whenever the change feed receive change events.
        /// The documents here are ordered by change date and are delivered on batches from a single partition.
        /// </summary>
        /// <param name="context">Change feed context</param>
        /// <param name="docs">The batch of changed documents</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            if (this._logger.IsEnabled(LogLevel.Trace))
            {
                this._logger.LogInformation($"Received {docs.Count} doc(s) from collection '{this._options.FeedCollectionInfo.CollectionName}'.");
            }

            var grain = this._grainFactory.GetGrain<IChangeFeedDispatcher>($"{this._name}-{context.PartitionKeyRangeId}");
            var processTask = Task.Factory.StartNew(
                async () => await grain.Dispatch(await this.BuildBatch(docs)),
                cancellationToken,
                TaskCreationOptions.None,
                this._scheduler
            );

            this._checkpointer.AddBatch(processTask, context);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Build a batch of messages to be dispatched to the subscribers
        /// </summary>
        /// <returns>The back of messages to be dispatched</returns>
        private async Task<Immutable<IReadOnlyList<(Guid StreamId, string StreamNamespace, Document Document)>>> BuildBatch(IReadOnlyList<Document> docs)
        {
            var batch = new List<(Guid StreamId, string StreamNamespace, Document Document)>();
            foreach (var d in docs)
            {
                var streamIdentity = await this._mapper.GetStreamIdentity(d);
                if (streamIdentity == default ||
                    streamIdentity.StreamId == Guid.Empty ||
                    string.IsNullOrWhiteSpace(streamIdentity.StreamNamespace))
                {
                    this._logger.LogWarning($"Invalid stream identity. The document will be skipped: '{JsonConvert.SerializeObject(d)}'");
                    continue;
                }
                batch.Add((streamIdentity.StreamId, streamIdentity.StreamNamespace, d));
            }
            return ((IReadOnlyList<(Guid StreamId, string StreamNamespace, Document Document)>)batch.AsReadOnly()).AsImmutable();
        }

        /// <summary>
        /// Begin monitoring the CosmosDB Change Feed
        /// </summary>
        public async Task Start()
        {
            this._processor = await new Microsoft.Azure.Documents.ChangeFeedProcessor.ChangeFeedProcessorBuilder()
                .WithHostName(this._siloName)
                .WithProcessorOptions(new Microsoft.Azure.Documents.ChangeFeedProcessor.ChangeFeedProcessorOptions
                {
                    CheckpointFrequency = new Microsoft.Azure.Documents.ChangeFeedProcessor.CheckpointFrequency
                    {
                        ExplicitCheckpoint = true
                    }
#if DEBUG
                    ,
                    LeasePrefix = "DEBUG"
#endif
                })
                .WithFeedCollection(this._options.FeedCollectionInfo)
                .WithLeaseCollection(this._options.LeaseCollectionInfo)
                .WithObserverFactory(this)
                .BuildAsync();
            await this._processor.StartAsync();
        }

        /// <summary>
        /// Stops monitoring the CosmosDB Change Feed
        /// </summary>
        public async Task Stop()
        {
            await this._processor.StopAsync();
        }

        public IChangeFeedObserver CreateObserver() => this;

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            this._logger.LogInformation($"Starting to monitor CosmosDB document collection '{this._options.FeedCollectionInfo.CollectionName}'. Silo: '{this._siloName}' | Partition key range: '{context.PartitionKeyRangeId}'");
            this._checkpointerTask = this._checkpointer.RunAsync(this._ctsClose.Token);
            return Task.CompletedTask;
        }

        public async Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            this._logger.LogInformation($"Stopping to monitor CosmosDB document collection '{this._options.FeedCollectionInfo.CollectionName}'. Silo: '{this._siloName}' | Partition key range: '{context.PartitionKeyRangeId}' | Reason: '{reason}'");
            this._ctsClose.Cancel();
            await this._checkpointerTask;
        }
    }
}