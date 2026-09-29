using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Serialization;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Serialization of the values that stores keep in documents. Types in
/// <see cref="DotCeleryJsonContext"/> use its generated code; others, such as values typed as
/// <see cref="object"/>, fall back to reflection.
/// </summary>
internal static class StoreJson
{
    private static readonly JsonSerializerOptions Options =
        JsonMessageSerializer.CreateCombinedOptions();

    public static JsonTypeInfo<T> TypeInfo<T>() => (JsonTypeInfo<T>)Options.GetTypeInfo(typeof(T));
}
