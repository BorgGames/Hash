namespace Hash;

sealed class PrioritySemaphore<T> {
    readonly PriorityQueue<TaskCompletionSource<bool>, T> queue = new();
    bool acquired;

    public async ValueTask WaitAsync(T priority, CancellationToken cancel = default) {
        TaskCompletionSource<bool> completion;
        lock (this.queue) {
            if (this.queue.Count == 0 && !this.acquired) {
                this.acquired = true;
                return;
            }

            completion = new();
            this.queue.Enqueue(completion, priority);
        }

        await completion.Task.WaitAsync(cancel).ConfigureAwait(false);
    }

    public void Release() {
        TaskCompletionSource<bool>? completion;
        lock (this.queue)
            if (!this.queue.TryDequeue(out completion, out _)) {
                if (!this.acquired)
                    throw new InvalidProgramException();
                this.acquired = false;
                return;
            }

        completion.TrySetResult(true);
    }
}
