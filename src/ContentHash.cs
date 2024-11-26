namespace Hash;

using System.Runtime.InteropServices;
using System.Security.Cryptography;

public readonly struct ContentHash(long a, long b, long c, long d): IEquatable<ContentHash> {
    readonly long a = a;
    readonly long b = b;
    readonly long c = c;
    readonly long d = d;

    public override string ToString() {
        var hex = new System.Text.StringBuilder(Marshal.SizeOf<ContentHash>() * 2);
        hex.Append(this.a.ToString("x16"));
        hex.Append(this.b.ToString("x16"));
        hex.Append(this.c.ToString("x16"));
        hex.Append(this.d.ToString("x16"));
        return hex.ToString();
    }

    public static ContentHash Compute(ReadOnlySpan<byte> bytes) {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(bytes, hash);
        return MemoryMarshal.Read<ContentHash>(hash);
    }

    public static ContentHash Fake(Random random) {
        if (random is null) throw new ArgumentNullException(nameof(random));

        Span<byte> hash = stackalloc byte[32];
        random.NextBytes(hash);
        return MemoryMarshal.Read<ContentHash>(hash);
    }

    public static unsafe ContentHash Compute(IntPtr ptr, int length)
        => Compute(new((void*)ptr, length));

    #region Equality
    public bool Equals(ContentHash other)
        => this.a == other.a && this.b == other.b && this.c == other.c && this.d == other.d;

    public override bool Equals(object? obj)
        => obj is ContentHash other && this.Equals(other);

    public override int GetHashCode() => this.a.GetHashCode();

    public static bool operator ==(ContentHash left, ContentHash right)
        => left.Equals(right);

    public static bool operator !=(ContentHash left, ContentHash right)
        => !left.Equals(right);
    #endregion
}