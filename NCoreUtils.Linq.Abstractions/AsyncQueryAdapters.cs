using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace NCoreUtils.Linq
{
    public static class AsyncQueryAdapters
    {
        private sealed class ByReferenceEqualityComparer<T> : IEqualityComparer<T>
            where T : class
        {
            public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

            public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
        }

        private static ByReferenceEqualityComparer<IAsyncQueryAdapter> RefEq { get; } = new();

        private static List<IAsyncQueryAdapter> Adapters { get; } = new();

        private static SpinLock _sync = new(enableThreadOwnerTracking: false);

        private static ref SpinLock Sync => ref _sync;

        public static void Add(IAsyncQueryAdapter adapter)
        {
            var lockTaken = false;
            try
            {
                Sync.Enter(ref lockTaken);
                if (!Adapters.Contains(adapter, RefEq))
                {
                    Adapters.Add(adapter);
                }
            }
            finally
            {
                if (lockTaken)
                {
                    Sync.Exit(useMemoryBarrier: false);
                }
            }
        }

        public static async ValueTask<IAsyncQueryProvider?> AdaptAsync(IQueryProvider provider, CancellationToken cancellationToken)
        {
            var lockTaken = false;
            try
            {
                Sync.Enter(ref lockTaken);
                if (0 == Adapters.Count)
                {
                    return default;
                }
                var i = 0;
                ValueTask<IAsyncQueryProvider> next()
                {
                    if (++i >= Adapters.Count)
                    {
                        return default;
                    }
                    return Adapters[i].GetAdapterAsync(next, provider, cancellationToken);
                }
                return await Adapters[0].GetAdapterAsync(next, provider, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (lockTaken)
                {
                    Sync.Exit(useMemoryBarrier: false);
                }
            }
        }
    }
}