namespace StreamNest;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Asynchronous streaming extension methods for <see cref="IAsyncEnumerable{T}"/>.
/// Provides convenient <c>ForEachAsync</c> and <c>ParallelForEachAsync</c> methods 
/// across multiple target frameworks (.NET Standard 2.0, 2.1, and .NET 10.0).
/// </summary>
public static class StreamExtensions
{
    /// <summary>
    /// Asynchronously enumerates each element in the sequence and executes the provided asynchronous action.
    /// </summary>
    public static async Task ForEachAsync<T>(
        this IAsyncEnumerable<T> source,
        Func<T, Task> action,
        CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(source, nameof(source));
        ExceptionHelper.ThrowIfNull(action, nameof(action));

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await action(item).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Asynchronously enumerates each element in the sequence and executes the provided action with cancellation support.
    /// </summary>
    public static async Task ForEachAsync<T>(
        this IAsyncEnumerable<T> source,
        Func<T, CancellationToken, Task> action,
        CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(source, nameof(source));
        ExceptionHelper.ThrowIfNull(action, nameof(action));

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await action(item, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Asynchronously enumerates each element in the sequence and executes a synchronous action.
    /// </summary>
    public static async Task ForEachAsync<T>(
        this IAsyncEnumerable<T> source,
        Action<T> action,
        CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(source, nameof(source));
        ExceptionHelper.ThrowIfNull(action, nameof(action));

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            action(item);
        }
    }

    /// <summary>
    /// Asynchronously processes elements of the sequence in parallel.
    /// Uses native <see cref="Parallel.ForEachAsync"/> on modern .NET runtimes, or an optimized throttled worker queue on .NET Standard.
    /// </summary>
    /// <param name="source">The source asynchronous enumerable.</param>
    /// <param name="action">The asynchronous worker action to invoke per element.</param>
    /// <param name="maxDegreeOfParallelism">Maximum concurrent tasks (defaults to processor count).</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    public static Task ParallelForEachAsync<T>(
        this IAsyncEnumerable<T> source,
        Func<T, Task> action,
        int maxDegreeOfParallelism = -1,
        CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(source, nameof(source));
        ExceptionHelper.ThrowIfNull(action, nameof(action));

#if NET6_0_OR_GREATER
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism > 0 ? maxDegreeOfParallelism : Environment.ProcessorCount,
            CancellationToken = cancellationToken
        };

        return Parallel.ForEachAsync(source, options, async (item, _) =>
        {
            await action(item).ConfigureAwait(false);
        });
#else
        return ParallelForEachAsyncCore(source, (item, ct) => action(item), maxDegreeOfParallelism, cancellationToken);
#endif
    }

    /// <summary>
    /// Asynchronously processes elements of the sequence in parallel with element cancellation tokens.
    /// Uses native <see cref="Parallel.ForEachAsync"/> on modern .NET runtimes, or an optimized throttled worker queue on .NET Standard.
    /// </summary>
    public static Task ParallelForEachAsync<T>(
        this IAsyncEnumerable<T> source,
        Func<T, CancellationToken, Task> action,
        int maxDegreeOfParallelism = -1,
        CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(source, nameof(source));
        ExceptionHelper.ThrowIfNull(action, nameof(action));

#if NET6_0_OR_GREATER
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism > 0 ? maxDegreeOfParallelism : Environment.ProcessorCount,
            CancellationToken = cancellationToken
        };

        return Parallel.ForEachAsync(source, options, async (item, ct) =>
        {
            await action(item, ct).ConfigureAwait(false);
        });
#else
        return ParallelForEachAsyncCore(source, action, maxDegreeOfParallelism, cancellationToken);
#endif
    }

#if !NET6_0_OR_GREATER
    private static async Task ParallelForEachAsyncCore<T>(
        IAsyncEnumerable<T> source,
        Func<T, CancellationToken, Task> action,
        int maxDegreeOfParallelism,
        CancellationToken cancellationToken)
    {
        int maxDop = maxDegreeOfParallelism > 0 ? maxDegreeOfParallelism : Environment.ProcessorCount;
        using var semaphore = new SemaphoreSlim(maxDop, maxDop);
        var tasks = new List<Task>();

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await action(item, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    semaphore.Release();
                }
            }, cancellationToken));

            if (tasks.Count >= maxDop * 4)
            {
                tasks.RemoveAll(t => t.IsCompleted);
            }
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }
#endif
}

