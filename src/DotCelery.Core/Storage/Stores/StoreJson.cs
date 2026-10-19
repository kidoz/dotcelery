using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Serialization;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Serialization of the values that stores keep in documents. The store's own records come from
/// <see cref="StoreJsonContext"/>, DotCelery's model types from
/// <see cref="DotCeleryJsonContext"/>, and a runtime type neither knows — such as a saga's
/// object-valued metadata — falls back to reflection, which a Native AOT application avoids by
/// not keeping arbitrary objects in a store record.
/// </summary>
internal static class StoreJson
{
    private static readonly JsonSerializerOptions Options = CreateStoreOptions();

    public static JsonTypeInfo<T> TypeInfo<T>() => (JsonTypeInfo<T>)Options.GetTypeInfo(typeof(T));

    private static JsonSerializerOptions CreateStoreOptions()
    {
        var options = JsonMessageSerializer.CreateCombinedOptions();

        // The store's own records are checked before DotCelery's model types and the reflection
        // fallback the combined options end with
        options.TypeInfoResolverChain.Insert(0, StoreJsonContext.Default);
        return options;
    }
}
