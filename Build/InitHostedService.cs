using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Hosting;
using MongoConsumer.DbCreation;
using MongoConsumer.Services;
using MongoLib.Documents;
using MongoLib.Init;

namespace MongoConsumer.Build
{
    internal sealed class InitHostedService : IHostedService
    {
        private readonly MongoDbInitializer _initializer;
        private readonly FieldCatalogSeedProvider _seedProvider;

        public InitHostedService
        (
            MongoDbInitializer initializer,
            FieldCatalogSeedProvider seedProvider
        )
        {
            _initializer = initializer;
            _seedProvider = seedProvider;
        }

        public async Task StartAsync(CancellationToken ct)
        {
            IEnumerable<FieldCatalogDocument> seed = _seedProvider.GetSeed();
            await _initializer.EnsureCreatedAsync(seed, ct);
        }

        public Task StopAsync(CancellationToken ct)
        {
            return Task.CompletedTask;
        }
    }
}
