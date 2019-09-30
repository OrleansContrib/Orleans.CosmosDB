using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Core;
using Orleans.Runtime;
using Orleans.Services;

namespace Orleans.Streaming.CosmosDB
{
    internal interface IChangeFeedService : IGrainService { }

    /// <summary>
    /// This GrainService hold a list of IProcessors and is the bridge between CosmosDB ChangeFeed and Orleans Streaming system.
    /// </summary>
    internal class ChangeFeedService : GrainService, IChangeFeedService
    {
        private readonly ILogger _logger;
        private readonly IReadOnlyList<IProcessor> _processors;

        public ChangeFeedService(
            CosmosDBStreamConfigurator configurator,
            IServiceProvider serviceProvider,
            IGrainIdentity id,
            IOptions<SiloOptions> siloOptions,
            Silo silo,
            ILoggerFactory loggerFactory)
            : base(id, silo, loggerFactory)
        {
            this._logger = loggerFactory.CreateLogger<ChangeFeedService>();

            var processor = new List<IProcessor>();
            foreach (var setting in configurator.Settings)
            {
                processor.Add(ChangeFeedProcessor.Create(
                    setting.Key,
                    siloOptions.Value.SiloName,
                    setting.Value.Options,
                    serviceProvider,
                    TaskScheduler.Current,
                    (IStreamMapper)ActivatorUtilities.CreateInstance(serviceProvider, setting.Value.MapperType)
                ));
            }

            this._processors = processor;
        }

        public override async Task Start()
        {
            this._logger.LogInformation($"Starting CosmosDB Change Feed stream provider registered processor(s).");
            foreach (var proc in this._processors)
            {
                this._logger.LogInformation($"Starting processor: {proc.Name}...");
                await proc.Start();
                this._logger.LogInformation($"Processor: {proc.Name} started.");
            }
            this._logger.LogInformation($"{this._processors.Count} processor(s) started.");

            await base.Start();
        }

        public override async Task Stop()
        {
            this._logger.LogInformation($"Stopping CosmosDB Change Feed stream provider registered processor(s).");
            foreach (var proc in this._processors)
            {
                this._logger.LogInformation($"Stopping processor: {proc.Name}...");
                await proc.Stop();
                this._logger.LogInformation($"Processor: {proc.Name} stopped.");
            }
            this._logger.LogInformation($"{this._processors.Count} processor(s) stopped.");

            await base.Stop();
        }
    }
}