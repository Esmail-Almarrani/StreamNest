namespace StreamNest;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// An asynchronous collection stream supporting multiple concurrent and nested contributors.
/// Implements standard <see cref="IAsyncEnumerable{T}"/> for native C# async streams and LINQ.
/// </summary>
/// <typeparam name="T">The type of elements in the stream.</typeparam>
public interface IAsyncCollectionStream<T> : 
    IAsyncEnumerable<T>,
    IAsyncDisposable, 
    IDisposable
{
    /// <summary>
    /// The number of currently active contributors (root and nested).
    /// </summary>
    int ActiveContributorsCount { get; }

    /// <summary>
    /// Total number of contributors registered since stream creation.
    /// </summary>
    int TotalContributorsCount { get; }

    /// <summary>
    /// Whether the stream has completed emitting all items.
    /// </summary>
    bool IsCompleted { get; }

    /// <summary>
    /// Gets a <see cref="Task"/> that completes when the stream has completed (or faulted).
    /// </summary>
    Task Completion { get; }

    /// <summary>
    /// Registers a new root contributor and increments the active contributor count.
    /// </summary>
    /// <param name="name">Optional name or label for the contributor.</param>
    /// <returns>A contributor handle.</returns>
    IStreamContributor<T> RegisterContributor(string? name = null);

    /// <summary>
    /// Runs a contributor delegate asynchronously. Automatically registers, executes,
    /// and completes/disposes the contributor.
    /// </summary>
    Task ProduceAsync(Func<IStreamContributor<T>, Task> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Seals the stream against new root contributors being registered via <see cref="RegisterContributor"/>.
    /// Once sealed, the stream will complete as soon as all currently active contributors complete.
    /// </summary>
    void Seal();

    /// <summary>
    /// Explicitly completes the stream.
    /// </summary>
    void Complete();

    /// <summary>
    /// Faults the stream with an exception, which will be propagated to consumers.
    /// </summary>
    void Fault(Exception exception);
}

