namespace StreamNest;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

/// <summary>
/// An asynchronous collection stream supporting multiple concurrent and nested contributors (producers).
/// The stream tracks all active contributors across the hierarchy and completes only when all
/// contributors (root and nested) have completed their work.
/// Fully compatible with standard async streams across multiple target frameworks (.NET Standard 2.0, 2.1, and .NET 10.0).
/// </summary>
/// <typeparam name="T">The type of elements in the stream.</typeparam>
public class AsyncCollectionStream<T> : IAsyncCollectionStream<T>
{
    private readonly Channel<T> _channel;
    private readonly AsyncCollectionStreamOptions _options;
    private int _activeContributorsCount;
    private int _totalContributorsCount;
    private bool _isSealed;
    private int _isDisposed;

    /// <summary>
    /// Initializes a new instance of <see cref="AsyncCollectionStream{T}"/>.
    /// </summary>
    /// <param name="options">Optional configuration options.</param>
    public AsyncCollectionStream(AsyncCollectionStreamOptions? options = null)
    {
        _options = options ?? new AsyncCollectionStreamOptions();

        if (_options.BoundedCapacity.HasValue && _options.BoundedCapacity.Value > 0)
        {
            var boundedOptions = new BoundedChannelOptions(_options.BoundedCapacity.Value)
            {
                FullMode = _options.FullMode,
                SingleReader = _options.SingleReader,
                SingleWriter = false
            };
            _channel = Channel.CreateBounded<T>(boundedOptions);
        }
        else
        {
            var unboundedOptions = new UnboundedChannelOptions
            {
                SingleReader = _options.SingleReader,
                SingleWriter = false
            };
            _channel = Channel.CreateUnbounded<T>(unboundedOptions);
        }
    }

    /// <inheritdoc/>
    public int ActiveContributorsCount => Volatile.Read(ref _activeContributorsCount);

    /// <inheritdoc/>
    public int TotalContributorsCount => Volatile.Read(ref _totalContributorsCount);

    /// <inheritdoc/>
    public bool IsCompleted => _channel.Reader.Completion.IsCompleted;

    /// <inheritdoc/>
    public Task Completion => _channel.Reader.Completion;

    /// <inheritdoc/>
    public IStreamContributor<T> RegisterContributor(string? name = null)
    {
        ThrowIfDisposed();

        if (_isSealed)
        {
            ExceptionHelper.ThrowInvalidOperation("Stream is sealed against new root contributors. Only existing contributors can spawn nested contributors.");
        }

        Interlocked.Increment(ref _activeContributorsCount);
        Interlocked.Increment(ref _totalContributorsCount);

        return new StreamContributor<T>(this, parent: null, name: name);
    }

    /// <inheritdoc/>
    public async Task ProduceAsync(Func<IStreamContributor<T>, Task> action, CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(action, nameof(action));
        ThrowIfDisposed();

        var contributor = RegisterContributor();
        try
        {
            await action(contributor).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            contributor.Fault(ex);
            throw;
        }
        finally
        {
            contributor.Complete();
        }
    }

    /// <inheritdoc/>
    public void Seal()
    {
        ThrowIfDisposed();
        _isSealed = true;

        CheckForCompletion();
    }

    /// <inheritdoc/>
    public void Complete()
    {
        _channel.Writer.TryComplete();
    }

    /// <inheritdoc/>
    public void Fault(Exception exception)
    {
        ExceptionHelper.ThrowIfNull(exception, nameof(exception));
        _channel.Writer.TryComplete(exception);
    }

    /// <summary>
    /// Emits an item from an active contributor into the stream channel.
    /// </summary>
    internal ValueTask EmitAsync(IStreamContributor<T> contributor, T item, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        return _channel.Writer.WriteAsync(item, cancellationToken);
    }

    /// <summary>
    /// Spawns a child contributor from an existing active contributor.
    /// </summary>
    internal IStreamContributor<T> CreateNestedContributor(IStreamContributor<T> parent, string? name)
    {
        ThrowIfDisposed();

        Interlocked.Increment(ref _activeContributorsCount);
        Interlocked.Increment(ref _totalContributorsCount);

        return new StreamContributor<T>(this, parent: parent, name: name);
    }

    /// <summary>
    /// Invoked when a contributor finishes its work.
    /// </summary>
    internal void OnContributorCompleted(IStreamContributor<T> contributor)
    {
        Interlocked.Decrement(ref _activeContributorsCount);
        CheckForCompletion();
    }

    /// <summary>
    /// Invoked when a contributor encounters an unhandled fault.
    /// </summary>
    internal void OnContributorFaulted(IStreamContributor<T> contributor, Exception exception)
    {
        Fault(exception);
    }

    private void CheckForCompletion()
    {
        if (_options.AutoCompleteWhenContributorsFinish && _isSealed && Volatile.Read(ref _activeContributorsCount) <= 0)
        {
            _channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Returns an asynchronous enumerator that iterates through the stream.
    /// </summary>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return new AsyncCollectionStreamEnumerator<T>(_channel.Reader, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        return default;
#else
        return new ValueTask(Task.CompletedTask);
#endif
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            _channel.Writer.TryComplete();
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _isDisposed) == 1)
        {
            ExceptionHelper.ThrowObjectDisposed(GetType().FullName ?? nameof(AsyncCollectionStream<T>));
        }
    }
}

/// <summary>
/// Static factories for creating and launching <see cref="IAsyncCollectionStream{T}"/> pipelines.
/// </summary>
public static class AsyncCollectionStream
{
    /// <summary>
    /// Creates an <see cref="IAsyncCollectionStream{T}"/> and launches a root contributor task that can spawn nested contributors.
    /// The stream is automatically sealed, so it will complete when the root contributor and all its
    /// spawned descendants complete.
    /// </summary>
    public static IAsyncCollectionStream<T> Create<T>(
        Func<IStreamContributor<T>, Task> rootProducer,
        AsyncCollectionStreamOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(rootProducer, nameof(rootProducer));

        var stream = new AsyncCollectionStream<T>(options);
        var contributor = stream.RegisterContributor("Root");
        stream.Seal();

        _ = Task.Run(async () =>
        {
            try
            {
                await rootProducer(contributor).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                contributor.Fault(ex);
            }
            finally
            {
                contributor.Complete();
            }
        }, cancellationToken);

        return stream;
    }

    /// <summary>
    /// Creates an <see cref="IAsyncCollectionStream{T}"/> and launches multiple concurrent root contributor tasks,
    /// each of which can spawn nested contributors.
    /// The stream is automatically sealed, so it will complete when all root contributors and all their
    /// spawned descendants complete.
    /// </summary>
    public static IAsyncCollectionStream<T> Create<T>(
        IEnumerable<Func<IStreamContributor<T>, Task>> rootProducers,
        AsyncCollectionStreamOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(rootProducers, nameof(rootProducers));

        var stream = new AsyncCollectionStream<T>(options);
        var contributorList = new List<(IStreamContributor<T> Contributor, Func<IStreamContributor<T>, Task> Producer)>();

        int index = 0;
        foreach (var producer in rootProducers)
        {
            var contributor = stream.RegisterContributor($"Root-{index++}");
            contributorList.Add((contributor, producer));
        }

        stream.Seal();

        foreach (var (contributor, producer) in contributorList)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await producer(contributor).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    contributor.Fault(ex);
                }
                finally
                {
                    contributor.Complete();
                }
            }, cancellationToken);
        }

        return stream;
    }
}

