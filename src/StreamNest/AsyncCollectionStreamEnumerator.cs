namespace StreamNest;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

/// <summary>
/// Bridges an internal <see cref="ChannelReader{T}"/> to <see cref="IAsyncEnumerator{T}"/>.
/// </summary>
/// <typeparam name="T">The type of elements in the stream.</typeparam>
public sealed class AsyncCollectionStreamEnumerator<T> : 
    IAsyncEnumerator<T>,
    IDisposable
{
    private readonly ChannelReader<T> _reader;
    private readonly CancellationToken _cancellationToken;
    private T _current = default!;
    private bool _hasCurrent;
    private bool _isDisposed;

    public AsyncCollectionStreamEnumerator(ChannelReader<T> reader, CancellationToken cancellationToken)
    {
        ExceptionHelper.ThrowIfNull(reader, nameof(reader));
        _reader = reader;
        _cancellationToken = cancellationToken;
    }

    /// <summary>
    /// Gets the element in the collection at the current position of the enumerator.
    /// </summary>
    public T Current
    {
        get
        {
            if (!_hasCurrent)
            {
                ExceptionHelper.ThrowInvalidOperation("Enumeration has not started or has already completed. Call MoveNextAsync first.");
            }
            return _current;
        }
    }

    /// <summary>
    /// Advances the enumerator asynchronously to the next element of the stream.
    /// </summary>
    public async ValueTask<bool> MoveNextAsync()
    {
        if (_isDisposed)
        {
            return false;
        }

        while (await _reader.WaitToReadAsync(_cancellationToken).ConfigureAwait(false))
        {
            if (_reader.TryRead(out var item))
            {
                _current = item;
                _hasCurrent = true;
                return true;
            }
        }

        _hasCurrent = false;
        _current = default!;
        return false;
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
        _isDisposed = true;
        _hasCurrent = false;
        _current = default!;
    }
}

