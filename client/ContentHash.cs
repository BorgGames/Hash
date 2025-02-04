namespace Hash;

using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;

public readonly struct ContentHash(long a, long b, long c, long d): IEquatable<ContentHash> {
    readonly long a = a;
    readonly long b = b;
    readonly long c = c;
    readonly long d = d;

    public const int SIZE_IN_BYTES = 4 * 8;

    public override string ToString() {
        var hex = new System.Text.StringBuilder(SIZE_IN_BYTES * 2);
        hex.Append(this.a.ToString("x16"));
        hex.Append(this.b.ToString("x16"));
        hex.Append(this.c.ToString("x16"));
        hex.Append(this.d.ToString("x16"));
        return hex.ToString();
    }

    public static ContentHash FromBytes(ReadOnlySpan<byte> bytes) {
        long a = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        long b = BinaryPrimitives.ReadInt64LittleEndian(bytes[8..]);
        long c = BinaryPrimitives.ReadInt64LittleEndian(bytes[16..]);
        long d = BinaryPrimitives.ReadInt64LittleEndian(bytes[24..]);
        return new(a, b, c, d);
    }

    public void WriteTo(Span<byte> bytes) {
        if (bytes.Length < SIZE_IN_BYTES)
            throw new ArgumentException("Buffer is too small", nameof(bytes));

        BinaryPrimitives.WriteInt64LittleEndian(bytes, this.a);
        BinaryPrimitives.WriteInt64LittleEndian(bytes[8..], this.b);
        BinaryPrimitives.WriteInt64LittleEndian(bytes[16..], this.c);
        BinaryPrimitives.WriteInt64LittleEndian(bytes[24..], this.d);
    }

    public static ContentHash Compute(SHA256 sha256, byte[] buffer, int offset, int count) {
        if (sha256 is null) throw new ArgumentNullException(nameof(sha256));

#if NET6_0_OR_GREATER
        Span<byte> hash = stackalloc byte[32];
        sha256.TryComputeHash(buffer.AsSpan(offset, count), hash, out _);
#else
        byte[] hash = sha256.ComputeHash(buffer, offset, count);
#endif
        return FromBytes(hash);
    }

    public static ContentHash Compute(SHA256 sha256, ReadOnlySpan<byte> buffer) {
        if (sha256 is null) throw new ArgumentNullException(nameof(sha256));

#if NET6_0_OR_GREATER
        Span<byte> hash = stackalloc byte[32];
        sha256.TryComputeHash(buffer, hash, out _);
#else
        byte[] tmp = ArrayPool<byte>.Shared.Rent(buffer.Length);
        buffer.CopyTo(tmp);
        byte[] hash = sha256.ComputeHash(tmp, 0, count: buffer.Length);
#endif
        return FromBytes(hash);
    }

#if NET6_0_OR_GREATER
    public static ContentHash Compute(ReadOnlySpan<byte> buffer) {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(buffer, destination: hash);
        return FromBytes(hash);
    }
    
    public static ContentHash Fake(Random random) {
        ArgumentNullException.ThrowIfNull(random);

        Span<byte> bytes = stackalloc byte[32];
        random.NextBytes(bytes);
        return FromBytes(bytes);
    }
#endif

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