namespace Hash;

using System.IO.MemoryMappedFiles;

static class MemoryMapExtensions {
    public static void Release(this MemoryMappedFile? file,
                               ref MemoryMappedViewAccessor? accessor) {
        if (accessor is null)
            return;

        accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        accessor.Dispose();
        accessor = null;
        file?.Dispose();
    }

    public static unsafe IntPtr AcquirePointer(this MemoryMappedViewAccessor accessor) {
        byte* pointer = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
        return (IntPtr)pointer;
    }
}