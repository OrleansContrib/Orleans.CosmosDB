using System;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Orleans.Runtime;
using System.Collections.Generic;
using System.Diagnostics;
using Orleans.Persistence.CosmosDB.Options;

namespace Orleans.Persistence.CosmosDB
{
    public class CosmosDBStorageOptions
    {
        private const string ORLEANS_DB = "Orleans";
        internal const string ORLEANS_STORAGE_COLLECTION = "OrleansStorage";
        private const int ORLEANS_STORAGE_COLLECTION_THROUGHPUT = 400;
        private const int ORLEANS_DATABASE_THROUGHPUT = 0;

        [Redact]
        public string AccountKey { get; set; }
        public string AccountEndpoint { get; set; }
        public string DB { get; set; } = ORLEANS_DB;

        /// <summary>
        /// Database configured throughput, if set to 0 it will not be configured and collection throughput must be set. See https://docs.microsoft.com/en-us/azure/cosmos-db/set-throughput 
        /// </summary>
        public int DatabaseThroughput { get; set; } = ORLEANS_STORAGE_COLLECTION_THROUGHPUT;
        public string Collection { get; set; } = ORLEANS_STORAGE_COLLECTION;
        /// <summary>
        /// RU units for collection, can be set to 0 if throughput is specified on database level. See https://docs.microsoft.com/en-us/azure/cosmos-db/set-throughput
        /// </summary>
        public int CollectionThroughput { get; set; } = ORLEANS_STORAGE_COLLECTION_THROUGHPUT;
        public bool CanCreateResources { get; set; }
        public bool DeleteStateOnClear { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public ConnectionMode ConnectionMode { get; set; } = ConnectionMode.Direct;

        [JsonConverter(typeof(StringEnumConverter))]
        public Protocol ConnectionProtocol { get; set; } = Protocol.Tcp;

        public JsonSerializerSettings JsonSerializerSettings { get; set; }

        public bool UseFullAssemblyNames { get; set; } = true;

        public bool IndentJson { get; set; } = true;

        [JsonConverter(typeof(StringEnumConverter))]
        public TypeNameHandling TypeNameHandling { get; set; } = TypeNameHandling.All;

        /// <summary>
        /// List of JSON path strings.
        /// Each entry on this list represents a property in the State Object that will be included in the document index.
        /// The default is to not add any property in the State object.
        /// </summary>
        public List<string> StateFieldsToIndex { get; set; } = new List<string>();

        /// <summary>
        /// Automatically add/update stored procudures on initialization.  This may result in slight downtime due to stored procedures having to be deleted and recreated in partitioned environments.
        /// Make sure this is false if you wish to strictly control downtime.
        /// </summary>
        public bool AutoUpdateStoredProcedures { get; set; }

        /// <summary>
        /// Delete the database on initialization.  Useful for testing scenarios.
        /// </summary>
        public bool DropDatabaseOnInit { get; set; }

        /// <summary>
        /// Stage of silo lifecycle where storage should be initialized.  Storage must be initialized prior to use.
        /// </summary>
        public int InitStage { get; set; } = DEFAULT_INIT_STAGE;

        public const int DEFAULT_INIT_STAGE = ServiceLifecycleStage.ApplicationServices;

        // TODO: Consistency level for emulator (defaults to Session; https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator)
        internal ConsistencyLevel? GetConsistencyLevel() => this.AccountEndpoint.Contains("localhost") ? (ConsistencyLevel?)ConsistencyLevel.Session : null;
    }

    /// <summary>
    /// Configuration validator for CosmosDBStorageOptions
    /// </summary>
    public class CosmosDBStorageOptionsValidator : IConfigurationValidator
    {
        private readonly CosmosDBStorageOptions options;
        private readonly string name;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">The option to be validated.</param>
        /// <param name="name">The option name to be validated.</param>
        public CosmosDBStorageOptionsValidator(CosmosDBStorageOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrWhiteSpace(this.options.DB))
                throw new OrleansConfigurationException(
                    $"Configuration for CosmosDBStorage {this.name} is invalid. {nameof(this.options.DB)} is not valid.");

            if (string.IsNullOrWhiteSpace(this.options.Collection))
                throw new OrleansConfigurationException(
                    $"Configuration for CosmosDBStorage {this.name} is invalid. {nameof(this.options.Collection)} is not valid.");

            if (this.options.CollectionThroughput < 400 && this.options.DatabaseThroughput < 400)
                throw new OrleansConfigurationException(
                    $"Configuration for CosmosDBStorage {this.name} is invalid. Either {nameof(this.options.DatabaseThroughput)} or {nameof(this.options.CollectionThroughput)} must exceed 400.");
        }
    }
}
