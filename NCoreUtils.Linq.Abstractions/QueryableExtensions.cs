using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace NCoreUtils.Linq
{
    public static class QueryableExtensions
    {
        #region warnings
        // see https://github.com/dotnet/runtime/blob/f6bd16d37c43dc5e9b89038f51d5b395f999efb8/src/libraries/System.Linq.Queryable/src/System/Linq/Queryable.cs#L13C9-L14C266

        internal const string InMemoryQueryableExtensionMethodsRequiresUnreferencedCode = "Enumerating in-memory collections as IQueryable can require unreferenced code because expressions referencing IQueryable extension methods can get rebound to IEnumerable extension methods. The IEnumerable extension methods could be trimmed causing the application to fail at runtime.";

        internal const string InMemoryQueryableExtensionMethodsRequiresDynamicCode = "Enumerating in-memory collections as IQueryable can require creating new generic types or methods, which requires creating code at runtime. This may not work when AOT compiling.";

        #endregion

        #region Helper methods to obtain MethodInfo in a safe way

        private static MethodInfo GetMethodInfo<TArg, TResult>(Func<TArg, TResult> f)
            => f.GetMethodInfo();

        private static MethodInfo GetMethodInfo<TArg1, TArg2, TResult>(Func<TArg1, TArg2, TResult> f)
            => f.GetMethodInfo();

        #endregion

        private static ValueTask<IAsyncQueryProvider> GetAsync(
            this IQueryProvider provider,
            CancellationToken cancellationToken)
        {
            if (provider == null)
            {
                throw new ArgumentNullException(nameof(provider));
            }
            if (provider is IAsyncQueryProvider asyncProvider)
            {
                return new ValueTask<IAsyncQueryProvider>(asyncProvider);
            }
            var adaptedProvider = AsyncQueryAdapters.AdaptAsync(provider, cancellationToken);
            if (adaptedProvider.IsCompletedSuccessfully)
            {
                return adaptedProvider.Result switch
                {
                    null => throw new InvalidOperationException($"{provider.GetType().FullName} cannot be adapted."),
                    var result => new(result)
                };
            }
            return new(ContinueAsync(provider, adaptedProvider.AsTask()));

            static async Task<IAsyncQueryProvider>ContinueAsync(IQueryProvider provider, Task<IAsyncQueryProvider?> valueTask)
            {
                var adaptedProvider = await valueTask.ConfigureAwait(false);
                if (null != adaptedProvider)
                {
                    return adaptedProvider;
                }
                throw new InvalidOperationException($"{provider.GetType().FullName} cannot be adapted.");
            }
        }

        public static Task<bool> AllAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            var negPredicate = Expression.Lambda<Func<T, bool>>(Expression.Not(predicate.Body), predicate.Parameters);
            return source.AnyAsync(negPredicate, cancellationToken);
        }

        public static async Task<bool> AnyAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<bool>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, bool>(Queryable.Any),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static Task<bool> AnyAsync<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            return source.Where(predicate).AnyAsync(cancellationToken);
        }

        public static async Task<bool> ContainsAsync<T>(this IQueryable<T> source, T item, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<bool>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, T, bool>(Queryable.Contains),
                source.Expression,
                Expression.Constant(item)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> CountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<int>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, int>(Queryable.Count),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static Task<int> CountAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            return source.Where(predicate).CountAsync(cancellationToken);
        }

        public static async Task<T> ElementAtAsync<T>(
            this IQueryable<T> source,
            int index,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<T>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, int, T>(Queryable.ElementAt),
                source.Expression,
                Expression.Constant(index)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<T?> ElementAtOrDefaultAsync<T>(
            this IQueryable<T> source,
            int index,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<T>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, int, T?>(Queryable.ElementAtOrDefault),
                source.Expression,
                Expression.Constant(index)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<T> FirstAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<T>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, T>(Queryable.First),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<T?> FirstOrDefaultAsync<T>(
            this IQueryable<T> source,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<T>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, T?>(Queryable.FirstOrDefault),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static Task<T> FirstAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            return source.Where(predicate).FirstAsync(cancellationToken);
        }

        public static Task<T?> FirstOrDefaultAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            return source.Where(predicate).FirstOrDefaultAsync(cancellationToken);
        }

        public static async Task<long> LongCountAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<long>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, long>(Queryable.LongCount),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static Task<long> LongCountAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            return source.Where(predicate).LongCountAsync(cancellationToken);
        }

        public static async Task<T> SingleAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<T>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, T>(Queryable.Single),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<T?> SingleOrDefaultAsync<T>(
            this IQueryable<T> source,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<T>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, T?>(Queryable.SingleOrDefault),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static Task<T> SingleAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            return source.Where(predicate).SingleAsync(cancellationToken);
        }

        public static Task<T?> SingleOrDefaultAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException(nameof(predicate));
            }
            return source.Where(predicate).SingleOrDefaultAsync(cancellationToken);
        }

        public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            List<T>? result = default;
            var items = asyncProvider.ExecuteEnumerableAsync<T>(source.Expression)
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false);
            await foreach (var item in items)
            {
                (result ??= []).Add(item);
            }
            return result switch
            {
                { Count: >0 } => [.. result],
                _ => []
            };
        }

        public static async Task<List<T>> ToListAsync<T>(this IQueryable<T> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            var result = new List<T>();
            var items = asyncProvider.ExecuteEnumerableAsync<T>(source.Expression)
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false);
            await foreach (var item in items)
            {
                result.Add(item);
            }
            return result;
        }

        public static async Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TKey, TElement>(
            this IQueryable<TElement> source,
            Func<TElement, TKey> keySelector,
            IEqualityComparer<TKey>? comparer,
            CancellationToken cancellationToken)
            where TKey : notnull
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException(nameof(keySelector));
            }
            var result = new Dictionary<TKey, TElement>(comparer ?? EqualityComparer<TKey>.Default);
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            var items = asyncProvider.ExecuteEnumerableAsync<TElement>(source.Expression)
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false);
            await foreach (var item in items)
            {
                result.Add(keySelector(item), item);
            }
            return result;
        }

        public static Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TKey, TElement>(
            this IQueryable<TElement> source,
            Func<TElement, TKey> keySelector,
            CancellationToken cancellationToken)
            where TKey : notnull
            => source.ToDictionaryAsync(keySelector, null, cancellationToken);

        public static async Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TKey, TElement, TSource>(
            this IQueryable<TSource> source,
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> valueSelector,
            IEqualityComparer<TKey>? comparer,
            CancellationToken cancellationToken)
            where TKey : notnull
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException(nameof(keySelector));
            }
            if (valueSelector == null)
            {
                throw new ArgumentNullException(nameof(valueSelector));
            }
            var result = new Dictionary<TKey, TElement>(comparer ?? EqualityComparer<TKey>.Default);
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            var items = asyncProvider.ExecuteEnumerableAsync<TSource>(source.Expression)
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false);
            await foreach (var item in items)
            {
                result.Add(keySelector(item), valueSelector(item));
            }
            return result;
        }

        public static Task<Dictionary<TKey, TElement>> ToDictionaryAsync<TKey, TElement, TSource>(
            this IQueryable<TSource> source,
            Func<TSource, TKey> keySelector,
            Func<TSource, TElement> valueSelector,
            CancellationToken cancellationToken)
            where TKey : notnull
            => source.ToDictionaryAsync(keySelector, valueSelector, null, cancellationToken);

        public static async IAsyncEnumerable<T> ExecuteAsync<T>(
            this IQueryable<T> source,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            var items = asyncProvider.ExecuteEnumerableAsync<T>(source.Expression)
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false);
            await foreach(var item in items)
            {
                yield return item;
            }
        }

        public static async Task<int> SumAsync(this IQueryable<int> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<int>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<int>, int>(Queryable.Sum),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> SumAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, int>> selector,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<int>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, Expression<Func<T, int>>, int>(Queryable.Sum),
                source.Expression,
                Expression.Quote(selector)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<long> SumAsync(this IQueryable<long> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<long>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<long>, long>(Queryable.Sum),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<long> SumAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, long>> selector,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<long>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, Expression<Func<T, long>>, long>(Queryable.Sum),
                source.Expression,
                Expression.Quote(selector)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<decimal> SumAsync(this IQueryable<decimal> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<decimal>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<decimal>, decimal>(Queryable.Sum),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<decimal> SumAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, decimal>> selector,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<decimal>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, Expression<Func<T, decimal>>, decimal>(Queryable.Sum),
                source.Expression,
                Expression.Quote(selector)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int?> SumAsync(this IQueryable<int?> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<int?>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<int?>, int?>(Queryable.Sum),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int?> SumAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, int?>> selector,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<int?>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, Expression<Func<T, int?>>, int?>(Queryable.Sum),
                source.Expression,
                Expression.Quote(selector)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<long?> SumAsync(this IQueryable<long?> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<long?>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<long?>, long?>(Queryable.Sum),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<long?> SumAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, long?>> selector,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<long>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, Expression<Func<T, long?>>, long?>(Queryable.Sum),
                source.Expression,
                Expression.Quote(selector)
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<decimal?> SumAsync(this IQueryable<decimal?> source, CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<decimal?>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<decimal?>, decimal?>(Queryable.Sum),
                source.Expression
            ), cancellationToken).ConfigureAwait(false);
        }

        public static async Task<decimal?> SumAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, decimal?>> selector,
            CancellationToken cancellationToken)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }
            if (selector is null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
            var asyncProvider = await source.Provider.GetAsync(cancellationToken).ConfigureAwait(false);
            return await asyncProvider.ExecuteAsync<decimal?>(Expression.Call(
                null,
                GetMethodInfo<IQueryable<T>, Expression<Func<T, decimal?>>, decimal?>(Queryable.Sum),
                source.Expression,
                Expression.Quote(selector)
            ), cancellationToken).ConfigureAwait(false);
        }
    }
}