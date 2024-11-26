#if NETSTANDARD2_0
// ReSharper disable once CheckNamespace
namespace System.Collections.Generic;

static class Dict {
    public static bool Remove<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key,
                                            out TValue value) where TKey : notnull {
        if (dict.TryGetValue(key, out value)) {
            dict.Remove(key);
            return true;
        }

        return false;
    }

    public static TValue GetValueOrDefault<TKey, TValue>(this Dictionary<TKey, TValue> dict,
                                                         TKey key, TValue @default)
        where TKey : notnull {
        return dict.TryGetValue(key, out var value) ? value : @default;
    }
}

#endif