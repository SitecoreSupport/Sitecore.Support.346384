using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Sitecore.Framework.Publishing;
using Sitecore.Framework.Publishing.DataPromotion;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.Locators;

namespace Sitecore.Support.Framework.Publishing.DataPromotion
{
    public class DefaultItemCloneManifestPromoter : Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter
    {
        public DefaultItemCloneManifestPromoter(Microsoft.Extensions.Logging.ILogger<Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter> logger, Sitecore.Framework.Publishing.DataPromotion.PromoterOptions options = null) : base(logger, options)
        {
        }

        public DefaultItemCloneManifestPromoter(Microsoft.Extensions.Logging.ILogger<Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter> logger, Microsoft.Extensions.Configuration.IConfiguration config) : base(logger, config)
        {
        }

        public DefaultItemCloneManifestPromoter(Microsoft.Extensions.Logging.ILogger<Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter> logger, IEqualityComparer<Sitecore.Framework.Publishing.Item.IItemVariantIdentifier> variantIdentifierComparer, Sitecore.Framework.Publishing.DataPromotion.PromoterOptions options = null) : base(logger, variantIdentifierComparer, options)
        {
        }

        public DefaultItemCloneManifestPromoter(Microsoft.Extensions.Logging.ILogger<Sitecore.Framework.Publishing.DataPromotion.DefaultItemCloneManifestPromoter> logger, IEqualityComparer<Sitecore.Framework.Publishing.Item.IItemVariantIdentifier> variantIdentifierComparer, Microsoft.Extensions.Configuration.IConfiguration config) : base(logger, variantIdentifierComparer, config)
        {
        }

        protected override async Task<IEnumerable<Tuple<IItemVariant, IItemRelationship[]>>> DecloneVariants(
            TargetPromoteContext targetContext,
            IItemReadRepository itemRepository,
            IItemRelationshipRepository relationshipRepository,
            IItemVariantLocator[] cloneLocators)
        {
            var cloneVariants = await itemRepository.GetVariants(cloneLocators).ConfigureAwait(false);

            var deepSourceVariants =
                await GetCloneSourcesRecursively(itemRepository, cloneVariants).ConfigureAwait(false);

            var clonesMap = BuildCloneVariantsMap(cloneVariants, deepSourceVariants);

            var allRelationships = await relationshipRepository
                .GetOutRelationships(targetContext.SourceStore.Name,
                                    Enumerable.Concat(cloneLocators, deepSourceVariants.Keys).Distinct(new IItemVariantIdentifierComparer()).ToArray())
                                        .ConfigureAwait(false);

            var result = new List<Tuple<IItemVariant, IItemRelationship[]>>();
            foreach (var cloneChain in clonesMap)
            {
                var cloneVariant = cloneChain.First();
                var itemData = cloneVariant.Properties;

                var mergedFields = cloneVariant.Fields
                    .Where(f => !IsCloneSourceField(f.FieldId))
                    .ToDictionary(f => f.FieldId, f => f);

                var mergedRelationships = GetVariantRelationships(allRelationships, cloneVariant)
                    .Where(x => !IsCloneSourceField(x.SourceFieldId))
                    .ToList();

                foreach (var variant in cloneChain.Skip(1)) 
                {
                    IReadOnlyCollection<IItemRelationship> variantRelationships = GetVariantRelationships(allRelationships, variant);

                    foreach (var field in variant.Fields)
                    {
                        if (IsCloneSourceField(field.FieldId))
                            continue;
                        if (mergedFields.ContainsKey(field.FieldId))
                            continue;

                        mergedFields.Add(
                            field.FieldId,
                            new FieldData(
                                    field.FieldId,
                                    cloneVariant.Id,
                                    field.RawValue,
                                    CreateFieldVarianceInfo(cloneVariant, field)));

                        var relationshipsOfMergedFields = variantRelationships
                            .Where(x => x.SourceFieldId == field.FieldId)
                            .Select(x => new ItemRelationship(
                               Guid.NewGuid(),
                               cloneVariant.Id,
                               x.TargetId,
                               x.Type,
                               field.Variance,
                               x.TargetVariance,
                               x.TargetPath,
                               x.SourceFieldId)).ToArray();

                        if (relationshipsOfMergedFields.Any())
                        {
                            mergedRelationships.AddRange(relationshipsOfMergedFields);
                        }
                    }
                }

                var item = new Tuple<IItemVariant, IItemRelationship[]>(
                    new ItemVariant(
                       cloneVariant.Id,
                       cloneVariant.Language,
                       cloneVariant.Version,
                       cloneVariant.Revision,
                       cloneVariant.BaseLastModified,
                       cloneVariant.VariantLastModified,
                       itemData,
                       mergedFields.Values.ToArray()),
                mergedRelationships.ToArray());

                result.Add(item);
            }
            return result;
        }

        private static List<List<IItemVariant>> BuildCloneVariantsMap(IEnumerable<IItemVariant> cloneVariants, Dictionary<IItemVariantIdentifier, IItemVariant> deepSourceVariants)
        {
            var clonesMap = new List<List<IItemVariant>>();
            foreach (var clone in cloneVariants)
            {
                var cloneSourceChain = new List<IItemVariant>();

                var currentVariant = clone;
                while (true)
                {
                    cloneSourceChain.Add(currentVariant);

                    IItemVariantLocator sourceVariantLocator;
                    if (!TryGetCloneSourceVariantUri(currentVariant, "not_important", out sourceVariantLocator) ||
                        !deepSourceVariants.TryGetValue(sourceVariantLocator, out currentVariant))
                    {
                        break;
                    }
                }
                clonesMap.Add(cloneSourceChain);
            }

            return clonesMap;
        }

        private static async Task<Dictionary<IItemVariantIdentifier, IItemVariant>> GetCloneSourcesRecursively(IItemReadRepository itemRepository, IEnumerable<IItemVariant> cloneVariants)
        {
            // get sources recursively
            var deepSourceVariants = new Dictionary<IItemVariantIdentifier, IItemVariant>(new IItemVariantIdentifierComparer());

            var clonesToProcess = cloneVariants.ToList();
            while (true)
            {
                var sourceUris = clonesToProcess.Select(v =>
                {
                    IItemVariantLocator cloneSourceUri;
                    if (TryGetCloneSourceVariantUri(v, "not_important", out cloneSourceUri))
                    {
                        return cloneSourceUri;
                    }
                    return null; 
                }).Where(v => v != null).ToArray();

                var sources = await itemRepository.GetVariants(sourceUris);

                clonesToProcess.Clear();

                foreach (var source in sources)
                {
                    if (!deepSourceVariants.ContainsKey(source))
                    {
                        deepSourceVariants.Add(source, source);
                    }
                    if (source.IsClone())
                    {
                        clonesToProcess.Add(source);
                    }
                }

                if (!clonesToProcess.Any()) break;
            }

            return deepSourceVariants;
        }

        private static IReadOnlyCollection<IItemRelationship> GetVariantRelationships(IDictionary<IItemVariantIdentifier, IReadOnlyCollection<IItemRelationship>> allRelationships, IItemVariant variant)
        {
            IReadOnlyCollection<IItemRelationship> variantRelationships;
            if (allRelationships.TryGetValue(variant, out variantRelationships))
            {
                return variantRelationships;
            }

            return new ReadOnlyCollection<IItemRelationship>(Enumerable.Empty<IItemRelationship>().ToArray()); //Wanted to use Array.Empty<>. But not available in net 452
        }

        private static bool IsCloneSourceField(Guid? fieldId)
        {
            return fieldId == PublishingConstants.Clones.SourceItem || fieldId == PublishingConstants.Clones.SourceVariant;
        }

        private static bool TryGetCloneSourceVariantUri(IItemVariant variant, string storeName, out IItemVariantLocator sourceUri)
        {
            sourceUri = null;
            var sourceField = variant.Fields.FirstOrDefault(x => x.FieldId == PublishingConstants.Clones.SourceVariant);
            var sourceRawUri = sourceField?.RawValue;

            if (!string.IsNullOrWhiteSpace(sourceRawUri))
            {
                sourceUri = ItemLocatorUtils.ParseSitecoreVariantUri(sourceRawUri, storeName);
                return true;
            }

            var sourceItemField = variant.Fields.FirstOrDefault(x => x.FieldId == PublishingConstants.Clones.SourceItem);
            var sourceItemUri = sourceItemField?.RawValue;

            if (!string.IsNullOrEmpty(sourceItemUri))
            {
                sourceUri = ItemLocatorUtils.ParseSitecoreVariantUri($"{sourceItemUri}?lang={variant.Language}&ver={variant.Version}", storeName);
                return true;
            }

            return false;
        }
    }
}