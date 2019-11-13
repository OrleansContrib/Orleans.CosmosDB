using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streaming.CosmosDB
{
    /// <summary>
    /// Configure CosmosDB stream provider
    /// </summary>
    public class CosmosDBStreamConfigurator
    {
        internal Dictionary<string, (CosmosDBStreamOptions Options, Type MapperType)> Settings;

        internal CosmosDBStreamConfigurator()
        {
            this.Settings = new Dictionary<string, (CosmosDBStreamOptions Options, Type MapperType)>();
        }

        /// <summary>
        /// Register CosmosDB Change Feed to the stream provider
        /// </summary>
        /// <param name="name">Provider Name</param>
        /// <param name="configure">Options configuration delegate</param>
        /// <param name="mapperType">(optional) A custom IStreamMapper implementation type.</param>
        public CosmosDBStreamConfigurator AddStream(string name, Action<CosmosDBStreamOptions> configure, Type mapperType = null)
        {
            if (configure == null) throw new ArgumentNullException(nameof(configure));

            if (mapperType == null)
            {
                mapperType = typeof(IdBasedStreamMapper);
            }
            else if (!mapperType.GetInterfaces().Contains(typeof(IStreamMapper)))
            {
                throw new InvalidOperationException("Mapper must implement 'IStreamMapper' interface'");
            }

            var options = new CosmosDBStreamOptions();
            configure.Invoke(options);
            this.Settings[name] = (options, mapperType);

            return this;
        }
    }
}