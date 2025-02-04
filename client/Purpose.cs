namespace Hash;

internal enum Purpose {
    READ,
    WRITE,
    AVAILABLE,
    EVICTED,
    ERROR,
}

internal static class PurposeExtensions {
    public static byte Byte(this Purpose purpose)
        => purpose switch {
            Purpose.READ => 0,
            Purpose.WRITE => 1,
            Purpose.AVAILABLE => 2,
            Purpose.EVICTED => 3,
            Purpose.ERROR => 4,
            _ => throw new ArgumentOutOfRangeException(nameof(purpose)),
        };
}