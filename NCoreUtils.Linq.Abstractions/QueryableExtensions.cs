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
        #region Helper methods to obtain MethodInfo in a safe way
        #pragma warning disable IDE0060,IDE0051
        private static MethodInfo GetMethodInfo<T1, T2>(Func<T1, T2> f, T1 unused1)
            => f.GetMethodInfo();
        private static MethodInfo GetMethodInfo<T1, T2, T3>(Func<T1, T2, T3> f, T1 unused1, T2 unused2)
            => f.GetMethodInfo();
        private static MethodInfo GetMethodInfo<T1, T2, T3, T4>(Func<T1, T2, T3, T4> f, T1 unused1, T2 unused2, T3 unused3)
            => f.GetMethodInfo();
        private static MethodInfo GetMethodInfo<T1, T2, T3, T4, T5>(Func<T1, T2, T3, T4, T5> f, T1 unused1, T2 unused2, T3 unused3, T4 unused4)
            => f.GetMethodInfo();
        private static MethodInfo GetMethodInfo<T1, T2, T3, T4, T5, T6>(Func<T1, T2, T3, T4, T5, T6> f, T1 unused1, T2 unused2, T3 unused3, T4 unused4, T5 unused5)
            => f.GetMethodInfo();
        private static MethodInfo GetMethodInfo<T1, T2, T3, T4, T5, T6, T7>(Func<T1, T2, T3, T4, T5, T6, T7> f, T1 unused1, T2 unused2, T3 unused3, T4 unused4, T5 unused5, T6 unused6)
            => f.GetMethodInfo();
        #pragma warning restore IDE0060,IDE0051
        #endregion

        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(Queryable))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(Enumerable))]
        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Dynamic dependency.")]
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
                GetMethodInfo(Queryable.Any, source),
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
                GetMethodInfo(Queryable.Contains, source, item),
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
                GetMethodInfo(Queryable.Count, source),
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
                GetMethodInfo(Queryable.ElementAt, source, index),
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
                GetMethodInfo(Queryable.ElementAtOrDefault, source, index),
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
                GetMethodInfo(Queryable.First, source),
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
                GetMethodInfo(Queryable.FirstOrDefault, source),
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
                GetMethodInfo(Queryable.LongCount, source),
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
                GetMethodInfo(Queryable.Single, source),
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
                GetMethodInfo(Queryable.SingleOrDefault, source),
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
            => await source.ToListAsync(cancellationToken).ConfigureAwait(false) switch
            {
                { Count: 0 } => Array.Empty<T>(),
                var list => list.ToArray()
            };

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
                GetMethodInfo(Queryable.Sum, source),
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
                GetMethodInfo(Queryable.Sum, source, selector),
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
                GetMethodInfo(Queryable.Sum, source),
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
                GetMethodInfo(Queryable.Sum, source, selector),
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
                GetMethodInfo(Queryable.Sum, source),
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
                GetMethodInfo(Queryable.Sum, source, selector),
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
                GetMethodInfo(Queryable.Sum, source),
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
                GetMethodInfo(Queryable.Sum, source, selector),
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
                GetMethodInfo(Queryable.Sum, source),
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
                GetMethodInfo(Queryable.Sum, source, selector),
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
                GetMethodInfo(Queryable.Sum, source),
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
                GetMethodInfo(Queryable.Sum, source, selector),
                source.Expression,
                Expression.Quote(selector)
            ), cancellationToken).ConfigureAwait(false);
        }
    }
}