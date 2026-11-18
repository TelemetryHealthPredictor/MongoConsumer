using System;
using System.Collections.Generic;
using System.Text;
using MongoLib;
using ParameterDataLib;

namespace MongoConsumer.Services
{
    internal class BatchHandler
    {
        private readonly ITelemetryWriter _writer;

        // This is just for logging
        private int batchesHandled = 0;

        public BatchHandler(ITelemetryWriter writer)
        {
            _writer = writer;
        }

        public async Task ProcessAsync(ParameterData[] batch, CancellationToken ct = default)
        {
            Console.WriteLine($"Handling batch " + ++batchesHandled);

            await _writer.WritePacketAsync(batch, ct);
        }
    }
}
