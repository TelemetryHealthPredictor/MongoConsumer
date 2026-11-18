using System;
using System.Collections.Generic;
using System.Text;
using BaselineBuilder.ICDHandling;
using MongoLib.Documents;
using ThpUtils.Naming;

namespace MongoConsumer.DbCreation
{
    public sealed class FieldCatalogSeedProvider
    {
        private readonly IcdDeserializer _icd;
        private readonly NamingData _namingData;

        public FieldCatalogSeedProvider(IcdDeserializer icd, NamingData namingData)
        {
            _icd = icd;
            _namingData = namingData;
        }

        public IEnumerable<FieldCatalogDocument> GetSeed()
        {
            List<FieldCatalogDocument> seed = new(_icd.ParameterCount);

            foreach (IcdParameter parameter in _icd.Parameters)
            {
                Boolean usedForThp = false;

                if (_namingData.ParameterNames.Contains(parameter.Name))
                {
                    usedForThp = true;
                }

                seed.Add(new FieldCatalogDocument
                {
                    Field = parameter.Name,
                    Unit = parameter.Unit.ToString(),
                    UsedForTHP = usedForThp,
                    RateHz = parameter.Correlator,
                });
            }

            return seed;
        }
    }
}
