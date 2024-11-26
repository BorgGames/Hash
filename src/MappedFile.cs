namespace Hash;

using System.IO.MemoryMappedFiles;

public sealed class MappedFile: IDisposable {
    readonly MemoryMappedViewAccessor accessor;
    readonly MemoryMappedFile file;

    public nint Length { get; }
    public IntPtr Address { get; private set; }
    public bool IsDisposed => this.Address == IntPtr.Zero;

    MappedFile(MemoryMappedFile file, MemoryMappedViewAccessor accessor, IntPtr address,
               FileStream stream) {
        this.file = file;
        this.accessor = accessor;
        this.Address = address != IntPtr.Zero
            ? address
            : throw new ArgumentNullException(nameof(address));
        this.Length = (nint)stream.Length;
    }

    public static MappedFile Create(FileStream stream, MemoryMappedFileAccess access) {
        ArgumentNullException.ThrowIfNull(stream);

        var file = MemoryMappedFile.CreateFromFile(stream, mapName: null, capacity: 0,
                                                   access, HandleInheritability.None,
                                                   leaveOpen: false);
        MemoryMappedViewAccessor? accessor = null;
        IntPtr address = IntPtr.Zero;
        try {
            accessor = file.CreateViewAccessor(offset: 0, size: stream.Length, access);
            address = accessor.AcquirePointer();
            return new(file, accessor, address, stream);
        } catch {
            if (address != IntPtr.Zero)
                accessor!.SafeMemoryMappedViewHandle.ReleasePointer();
            accessor?.Dispose();
            file.Dispose();
            throw;
        }
    }

    public async ValueTask Flush(CancellationToken cancel = default) {
        ObjectDisposedException.ThrowIf(this.IsDisposed, this);
        await Task.Run(this.accessor.Flush, cancel).ConfigureAwait(false);
    }

    public void Dispose() {
        if (this.Address != IntPtr.Zero) {
            this.accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            this.Address = IntPtr.Zero;
        }

        this.accessor.Dispose();
        this.file.Dispose();
    }
}