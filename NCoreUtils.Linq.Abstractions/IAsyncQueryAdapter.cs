using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NCoreUtils.Linq;

public interface IAsyncQueryAdapter
{
    ValueTask<IAsyncQueryProvider> GetAdapterAsync(
        Func<ValueTask<IAsyncQueryProvider>> next,
        IQueryProvider source,
        CancellationToken cancellationToken = default
    );
}