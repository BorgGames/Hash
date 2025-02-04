namespace Hash;

readonly struct ContentStreamPacketFormat {
    public int PurposeBytes { get; private init; }
    public int SizeBytes { get; private init; }
    public int QueryBytes { get; private init; }
    public int WriteResponseLength => this.PurposeBytes + 8 + this.QueryBytes;

    public static ContentStreamPacketFormat V0(int maxBlockSize) {
        int purposeBytes = maxBlockSize switch {
            < sbyte.MaxValue => 1,
            < short.MaxValue => 2,
            < int.MaxValue => 4,
            _ => 8,
        };
        return new() {
            PurposeBytes = purposeBytes,
            SizeBytes = purposeBytes,
            QueryBytes = 4,
        };
    }

    public int WriteQueryLength(int dataBytes)
        => this.PurposeBytes + this.SizeBytes + this.QueryBytes
         + ContentHash.SIZE_IN_BYTES
         + dataBytes;
    
    public int ReadResponseLength(int dataBytes)
        => this.PurposeBytes + this.SizeBytes + this.QueryBytes
         + dataBytes;
}