namespace StreamNest;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Default implementation of <see cref="IStreamContributor{T}"/>.
/// </summary>
/// <typeparam name="T">The type of elements in the stream.</typeparam>
public class StreamContributor<T> : IStreamContributor<T>
{
    private readonly AsyncCollectionStream<T> _stream;
    private int _isCompleted; // 0 = active, 1 = completed

    internal StreamContributor(
        AsyncCollectionStream<T> stream, 
        IStreamContributor<T>? parent = null, 
        string? name = null)
    {
        ExceptionHelper.ThrowIfNull(stream, nameof(stream));
        _stream = stream;
        Parent = parent;
        Depth = parent == null ? 0 : parent.Depth + 1;
        Id = Guid.NewGuid().ToString("N").Substring(0, 8);
        Name = !string.IsNullOrEmpty(name) ? name! : $"Contributor-{Id}(d={Depth})";
    }

    public string Id { get; }

    public string Name { get; }

    public int Depth { get; }

    public IStreamContributor<T>? Parent { get; }

    public bool IsCompleted => Volatile.Read(ref _isCompleted) == 1;

    public ValueTask EmitAsync(T item, CancellationToken cancellationToken = default)
    {
        ThrowIfCompleted();
        return _stream.EmitAsync(this, item, cancellationToken);
    }

    public async ValueTask EmitRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(items, nameof(items));
        ThrowIfCompleted();

        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await EmitAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask EmitRangeAsync(IAsyncEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(items, nameof(items));
        ThrowIfCompleted();

        await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await EmitAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }

    public IStreamContributor<T> CreateNestedContributor(string? name = null)
    {
        ThrowIfCompleted();
        return _stream.CreateNestedContributor(this, name);
    }

    public async Task ProduceNestedAsync(Func<IStreamContributor<T>, Task> action, CancellationToken cancellationToken = default)
    {
        ExceptionHelper.ThrowIfNull(action, nameof(action));
        ThrowIfCompleted();

        var nested = CreateNestedContributor();
        try
        {
            await action(nested).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            nested.Fault(ex);
            throw;
        }
        finally
        {
            nested.Complete();
        }
    }

    public void Complete()
    {
        if (Interlocked.Exchange(ref _isCompleted, 1) == 0)
        {
            _stream.OnContributorCompleted(this);
        }
    }

    public void Fault(Exception exception)
    {
        ExceptionHelper.ThrowIfNull(exception, nameof(exception));

        if (Interlocked.Exchange(ref _isCompleted, 1) == 0)
        {
            _stream.OnContributorFaulted(this, exception);
        }
    }

    public ValueTask DisposeAsync()
    {
        Complete();
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        return default;
#else
        return new ValueTask(Task.CompletedTask);
#endif
    }

    public void Dispose()
    {
        Complete();
    }

    private void ThrowIfCompleted()
    {
        if (IsCompleted)
        {
            ExceptionHelper.ThrowInvalidOperation($"Contributor '{Name}' has already completed or faulted and cannot emit or spawn new contributors.");
        }
    }

    public override string ToString() => $"{Name} (Depth={Depth}, Active={!IsCompleted})";
}

