namespace Hash.Compat;

using System.Diagnostics.CodeAnalysis;

static class Dict {
    public static bool Remove<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key, [MaybeNullWhen(false)] out TValue value) {
        if (dict.TryGetValue(key, out value)) {
            dict.Remove(key);
            return true;
        }

        return false;
    }
}