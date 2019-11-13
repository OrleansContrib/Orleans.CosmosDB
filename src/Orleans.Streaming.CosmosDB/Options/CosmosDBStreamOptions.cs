using System;
using Microsoft.Azure.Documents.ChangeFeedProcessor;

namespace Orleans.Streaming.CosmosDB
{
    public class CosmosDBStreamOptions
    {
        /// <summary>
        /// The connection settings for the CosmosDB Document Collection tha will be monitored.
        /// </summary>
        public DocumentCollectionInfo FeedCollectionInfo { get; set; } = new DocumentCollectionInfo();

        /// <summary>
        /// The connection settings for the CosmosDB Change Feed Lease Collection.
        /// </summary>
        public DocumentCollectionInfo LeaseCollectionInfo { get; set; } = new DocumentCollectionInfo();

        /// <summary>
        /// The desired interval between checkpoints with the CosmosDB Change Feed.
        /// </summary>
        public TimeSpan CheckpointInterval { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Maximum amount of time to process stream documents.
        /// If this amount is exceed, an alert will be writen to the output logs.
        /// Default to 30 seconds.
        /// </summary>
        public TimeSpan MaxLatencyThreshold { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// If no custom implementation type of IStreamMapper is provided, this property will be used to define the Stream namespace.
        /// </summary>
        public string DefaultStreamNamespace { get; set; } = "Documents";
    }
}