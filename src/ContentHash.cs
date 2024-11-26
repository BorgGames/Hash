namespace Hash;

using System.Runtime.InteropServices;
using System.Security.Cryptography;

public readonly struct ContentHash: IEquatable<ContentHash> {
    readonly long a;
    readonly long b;
    readonly long c;
    readonly long d;

    public ContentHash(long a, long b, long c, long d) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
    }

    public override string ToString() {
        var hex = new System.Text.StringBuilder(Marshal.SizeOf<ContentHash>() * 2);
        hex.Append(this.a.ToString("x16"));
        hex.Append(this.b.ToString("x16"));
        hex.Append(this.c.ToString("x16"));
        hex.Append(this.d.ToString("x16"));
        return hex.ToString();
    }

    public static ContentHash Compute(ReadOnlySpan<byte> bytes) {
#if NET6_0_OR_GREATER
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(bytes, hash);
#else
        using var sha = SHA256.Create();
        byte[] hash = sha.ComputeHash(bytes.ToArray());
#endif
        return MemoryMarshal.Read<ContentHash>(hash);
    }

    public ReadOnlySpan<byte> Span
#if NET6_0_OR_GREATER
        => MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref this, 1));
#else
    {
        get {
            unsafe {
                fixed (ContentHash* ptr = &this)
                    return new ReadOnlySpan<byte>(ptr, sizeof(ContentHash));
            }
        }
    }
#endif

    public static ContentHash Fake(Random random) {
        if (random is null) throw new ArgumentNullException(nameof(random));

#if NET6_0_OR_GREATER
        Span<byte> hash = stackalloc byte[32];
        random.NextBytes(hash);
        return MemoryMarshal.Read<ContentHash>(hash);
#else
        byte[] hash = new byte[32];
        random.NextBytes(hash);
        return new(
            BitConverter.ToInt64(hash, 0),
            BitConverter.ToInt64(hash, 8),
            BitConverter.ToInt64(hash, 16),
            BitConverter.ToInt64(hash, 24)
        );
#endif
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