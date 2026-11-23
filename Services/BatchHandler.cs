using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoLib;
using ParameterDataLib;

namespace MongoConsumer.Services
{
    internal class BatchHandler
    {
        private const ulong FlightGapMs = 2UL * 60UL * 1000UL;
        private const int MaxPendingPackets = 1024;

        private readonly ITelemetryWriter _writer;
        private readonly List<ParameterData[]> _pendingPackets = [];

        private string? _currentFlightId;
        private ulong? _latestTimestampMs;

        // This is just for logging
        private int batchesHandled = 0;

        public BatchHandler(ITelemetryWriter writer)
        {
            _writer = writer;
        }

        public async Task ProcessAsync(ParameterData[] batch, CancellationToken ct = default)
        {
            Console.WriteLine($"Handling batch " + ++batchesHandled);

            if (batch is null || batch.Length == 0)
            {
                return;
            }

            ulong timestampMs = batch[0].TimestampMS;

            // Current flight exists and this packet continues it (0 to 2 minutes after the latest timestamp)
            if (_currentFlightId is not null
                && _latestTimestampMs is ulong latestMs
                && timestampMs >= latestMs
                && timestampMs - latestMs <= FlightGapMs)
            {
                _pendingPackets.Add(batch);
                _latestTimestampMs = timestampMs;

                if (_pendingPackets.Count >= MaxPendingPackets)
                {
                    await FlushAsync(ct);
                }

                return;
            }

            // No current flight, or the packet doesn't continue it: save what's pending, then
            // let MongoLib resolve this packet as if there were no current flight
            await FlushAsync(ct);

            string? flightId = await _writer.WritePacketAsync(batch, ct);

            if (flightId is null)
            {
                _currentFlightId = null;
                _latestTimestampMs = null;
            }
            else
            {
                _currentFlightId = flightId;
                _latestTimestampMs = timestampMs;
            }
        }

        public async Task FlushAsync(CancellationToken ct = default)
        {
            if (_currentFlightId is null || _pendingPackets.Count == 0)
            {
                return;
            }

            _pendingPackets.Clear();
            await _writer.WritePacketsAsync(_currentFlightId, _pendingPackets, ct);
        }
    }
}