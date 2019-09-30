using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using Microsoft.Extensions.Logging;

namespace Orleans.Streaming.CosmosDB
{
    /// <summary>
    /// This class describes a time-based checkpoint.
    /// So, whatever is set to 'CheckpointInterval' defined on the provider options, will be the time the checkpointer will await to checkpoint successful batches.
    /// </summary>
    internal class ChangeFeedCheckpointer
    {
        private readonly TimeSpan _interval;
        private readonly ILogger _logger;
        private readonly ConcurrentQueue<BatchRegistry> _batches;

        public ChangeFeedCheckpointer(ILoggerFactory loggerFactory, CosmosDBStreamOptions options)
        {
            this._interval = options.CheckpointInterval;
            this._logger = loggerFactory.CreateLogger<ChangeFeedCheckpointer>();
            this._batches = new ConcurrentQueue<BatchRegistry>();
        }

        /// <summary>
        /// Start the checkpointer loop that checks for completed batch tasks periodically in order to perform the checkpoints.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        public async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await CheckpointLastCompletedBatchAsync();

                try
                {
                    await Task.Delay(this._interval, cancellationToken);
                }
                catch (TaskCanceledException) { }
            }

            await CheckpointLastCompletedBatchAsync();
        }

        /// <summary>
        /// Try to checkpoint all the completed checkpoint batch tasks.
        /// It is called periodically by the main loop.
        /// </summary>
        private async Task CheckpointLastCompletedBatchAsync()
        {
            try
            {
                BatchRegistry lastCompletedRegistry = null;

                while (this._batches.TryPeek(out var batchTask))
                {
                    if (batchTask.IsCompleted)
                    {
                        this._batches.TryDequeue(out batchTask);
                        lastCompletedRegistry = batchTask;
                    }
                    else
                    {
                        break;
                    }
                }

                if (lastCompletedRegistry != null)
                {
                    this._logger.LogInformation("Checkpointing...");
                    await lastCompletedRegistry.CheckpointNow();
                }
            }
            catch (Exception ex)
            {
                this._logger.LogError(ex, "Error occured while executing checkpoint.");
            }
        }

        /// <summary>
        /// Add a processing batch task to be checkpointed later on.
        /// </summary>
        /// <param name="task">The batck task</param>
        /// <param name="context">The Change Feed context</param>
        public void AddBatch(Task task, IChangeFeedObserverContext context)
        {
            this._batches.Enqueue(new BatchRegistry(task, context));
        }
    }
}