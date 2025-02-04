namespace Hash;

struct ReadRequest {
    public uint ID;
    public long Offset;
    public int Length;
    public ContentHash Hash;
}