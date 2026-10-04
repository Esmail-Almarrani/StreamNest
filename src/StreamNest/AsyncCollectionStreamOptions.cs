namespace StreamNest;

using System.Threading.Channels;

/// <summary>
/// Configuration options for <see cref="AsyncCollectionStream{T}"/>.
/// </summary>
public class AsyncCollectionStreamOptions
{
    /// <summary>
    /// If set to a positive integer, the stream will use a bounded buffer of this size,
    /// providing backpressure to contributors. If null (default), the buffer is unbounded.
    /// </summary>
    public int? BoundedCapacity { get; set; } = null;

    /// <summary>
    /// The behavior when a bounded buffer is full. Defaults to <see cref="BoundedChannelFullMode.Wait"/>.
    /// </summary>
    public BoundedChannelFullMode FullMode { get; set; } = BoundedChannelFullMode.Wait;

    /// <summary>
    /// If true (default), the stream will automatically close and complete when all active contributors
    /// have finished (and the stream has been sealed).
    /// </summary>
    public bool AutoCompleteWhenContributorsFinish { get; set; } = true;

    /// <summary>
    /// If true (default), optimizes the internal channel for a single concurrent consumer reader.
    /// </summary>
    public bool SingleReader { get; set; } = true;
}

