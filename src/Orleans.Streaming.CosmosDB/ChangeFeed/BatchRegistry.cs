using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;

namespace Orleans.Streaming.CosmosDB
{
    /// <summary>
    /// A wrapper around the processing Task.
    /// </summary>
    public class BatchRegistry
    {
        private readonly Task _batch;
        private readonly IChangeFeedObserverContext _context;

        public bool IsCompleted => this._batch.IsCompleted && !this._batch.IsFaulted;

        public BatchRegistry(Task batch, IChangeFeedObserverContext context)
        {
            this._batch = batch ?? throw new ArgumentNullException(nameof(batch));
            this._context = context ?? throw new ArgumentNullException(nameof(context));
        }

        /// <summary>
        /// Tell the CosmosDB underlying Change Feed context to checkpoint.
        /// </summary>
        /// <returns></returns>
        public Task CheckpointNow() => this._context.CheckpointAsync();
    }
}