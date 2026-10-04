namespace StreamNest;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Represents a contributor (producer) that can emit items into an <see cref="IAsyncCollectionStream{T}"/>
/// and spawn nested (child) contributors.
/// </summary>
/// <typeparam name="T">The type of elements in the stream.</typeparam>
public interface IStreamContributor<T> : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Unique identifier for this contributor.
    /// </summary>
    string Id { get; }

    /// <summary>
    /// Optional human-readable name or label for this contributor.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// The nesting depth of this contributor (0 for root contributors, 1 for immediate children, etc.).
    /// </summary>
    int Depth { get; }

    /// <summary>
    /// Gets the parent contributor, or null if this is a root contributor.
    /// </summary>
    IStreamContributor<T>? Parent { get; }

    /// <summary>
    /// Gets whether this contributor has completed or faulted.
    /// </summary>
    bool IsCompleted { get; }

    /// <summary>
    /// Emits a single item into the stream.
    /// </summary>
    ValueTask EmitAsync(T item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Emits a sequence of items into the stream.
    /// </summary>
    ValueTask EmitRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken = default);

    /// <summary>
    /// Emits items from an asynchronous stream into the stream.
    /// </summary>
    ValueTask EmitRangeAsync(IAsyncEnumerable<T> items, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a child (nested) contributor. The stream's active contributor count is incremented,
    /// and the stream will not complete until this nested contributor (and all its descendants) complete.
    /// </summary>
    IStreamContributor<T> CreateNestedContributor(string? name = null);

    /// <summary>
    /// Spawns and executes an asynchronous nested contributor action. The child contributor is automatically
    /// completed when the task finishes, or faulted if the task throws.
    /// </summary>
    Task ProduceNestedAsync(Func<IStreamContributor<T>, Task> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks this contributor as completed, decrementing the stream's active contributor count.
    /// </summary>
    void Complete();

    /// <summary>
    /// Marks this contributor as faulted, propagating the failure to the stream.
    /// </summary>
    void Fault(Exception exception);
}

