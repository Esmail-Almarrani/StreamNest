namespace StreamNest.Tests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using StreamNest;
using NUnit.Framework;

[TestFixture]
public class NestedAsyncCollectionStreamTests
{
    [Test]
    public void ExceptionHelper_ThrowIfNull_ThrowsWhenNull()
    {
        string? nullString = null;
        var ex = Assert.Throws<ArgumentNullException>(() =>
        {
            ExceptionHelper.ThrowIfNull(nullString, nameof(nullString));
        });

        Assert.That(ex!.ParamName, Is.EqualTo(nameof(nullString)));
    }

    [Test]
    public void ExceptionHelper_ThrowIfNull_DoesNotThrowWhenNotNull()
    {
        string notNull = "Hello";
        Assert.DoesNotThrow(() =>
        {
            ExceptionHelper.ThrowIfNull(notNull, nameof(notNull));
        });
    }

    [Test]
    public void ExceptionHelper_ThrowIfNullOrWhiteSpace_ValidatesCorrectly()
    {
        Assert.Throws<ArgumentException>(() => ExceptionHelper.ThrowIfNullOrWhiteSpace(null, "param"));
        Assert.Throws<ArgumentException>(() => ExceptionHelper.ThrowIfNullOrWhiteSpace("", "param"));
        Assert.Throws<ArgumentException>(() => ExceptionHelper.ThrowIfNullOrWhiteSpace("   ", "param"));
        Assert.DoesNotThrow(() => ExceptionHelper.ThrowIfNullOrWhiteSpace("valid", "param"));
    }

    [Test]
    public async Task SingleContributor_EmitsItems_StreamCompletes()
    {
        var stream = AsyncCollectionStream.Create<int>(async contributor =>
        {
            await contributor.EmitAsync(1);
            await contributor.EmitAsync(2);
            await contributor.EmitAsync(3);
        });

        var results = new List<int>();
        await foreach (var item in stream)
        {
            results.Add(item);
        }

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(stream.IsCompleted, Is.True);
    }

    [Test]
    public async Task MultipleRootContributors_EmitConcurrently_AllItemsReceived()
    {
        var contributors = new List<Func<IStreamContributor<string>, Task>>
        {
            async c =>
            {
                await Task.Delay(10);
                await c.EmitAsync("A1");
                await c.EmitAsync("A2");
            },
            async c =>
            {
                await Task.Delay(5);
                await c.EmitAsync("B1");
                await c.EmitAsync("B2");
            },
            async c =>
            {
                await c.EmitAsync("C1");
                await c.EmitAsync("C2");
            }
        };

        var stream = AsyncCollectionStream.Create(contributors);

        var results = new ConcurrentBag<string>();
        await foreach (var item in stream)
        {
            results.Add(item);
        }

        Assert.That(results.Count, Is.EqualTo(6));
        Assert.That(results, Does.Contain("A1"));
        Assert.That(results, Does.Contain("A2"));
        Assert.That(results, Does.Contain("B1"));
        Assert.That(results, Does.Contain("B2"));
        Assert.That(results, Does.Contain("C1"));
        Assert.That(results, Does.Contain("C2"));
        Assert.That(stream.IsCompleted, Is.True);
    }

    [Test]
    public async Task NestedContributors_HierarchicalTree_StreamWaitsForAllDescendants()
    {
        var stream = AsyncCollectionStream.Create<string>(async root =>
        {
            await root.EmitAsync("Root-Start");

            var child1 = root.CreateNestedContributor("Child-1");
            _ = Task.Run(async () =>
            {
                try
                {
                    await child1.EmitAsync("Child1-Start");

                    var grandchild1 = child1.CreateNestedContributor("Grandchild-1");
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await Task.Delay(60);
                            await grandchild1.EmitAsync("Grandchild1-Item");
                        }
                        finally
                        {
                            grandchild1.Complete();
                        }
                    });

                    await child1.EmitAsync("Child1-End");
                }
                finally
                {
                    child1.Complete();
                }
            });

            await root.ProduceNestedAsync(async child2 =>
            {
                await Task.Delay(20);
                await child2.EmitAsync("Child2-Item");
            });

            await root.EmitAsync("Root-End");
        });

        var results = new List<string>();
        await foreach (var item in stream)
        {
            results.Add(item);
        }

        Assert.That(results, Does.Contain("Root-Start"));
        Assert.That(results, Does.Contain("Root-End"));
        Assert.That(results, Does.Contain("Child1-Start"));
        Assert.That(results, Does.Contain("Child1-End"));
        Assert.That(results, Does.Contain("Child2-Item"));
        Assert.That(results, Does.Contain("Grandchild1-Item"));
        Assert.That(results.Count, Is.EqualTo(6));
        Assert.That(stream.IsCompleted, Is.True);
    }

    [Test]
    public async Task StreamExtensions_ForEachAsync_ConsumesStreamSequentially()
    {
        var stream = AsyncCollectionStream.Create<int>(async contributor =>
        {
            for (int i = 1; i <= 5; i++)
            {
                await contributor.EmitAsync(i);
            }
        });

        var received = new List<int>();

        await stream.ForEachAsync(async number =>
        {
            await Task.Yield();
            received.Add(number);
        });

        Assert.That(received, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task StreamExtensions_ParallelForEachAsync_ConsumesInParallel()
    {
        var stream = AsyncCollectionStream.Create<int>(async contributor =>
        {
            for (int i = 1; i <= 10; i++)
            {
                await contributor.EmitAsync(i);
            }
        });

        var received = new ConcurrentBag<int>();

        await stream.ParallelForEachAsync(async number =>
        {
            await Task.Delay(5);
            received.Add(number);
        }, maxDegreeOfParallelism: 4);

        Assert.That(received.Count, Is.EqualTo(10));
        var sorted = new List<int>(received);
        sorted.Sort();
        Assert.That(sorted, Is.EqualTo(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }));
    }

    [Test]
    public async Task NativeLinq_ToListAsync_And_ToArrayAsync_Work()
    {
        var stream = AsyncCollectionStream.Create<int>(async contributor =>
        {
            await contributor.EmitRangeAsync(new[] { 10, 20, 30, 40 });
        });

        var list = await stream.ToListAsync();
        Assert.That(list, Is.EqualTo(new[] { 10, 20, 30, 40 }));

        var stream2 = AsyncCollectionStream.Create<int>(async contributor =>
        {
            await contributor.EmitRangeAsync(new[] { 50, 60 });
        });

        var array = await stream2.ToArrayAsync();
        Assert.That(array, Is.EqualTo(new[] { 50, 60 }));
    }

    [Test]
    public async Task NativeLinq_Where_Select_Count_Work()
    {
        var stream = AsyncCollectionStream.Create<int>(async contributor =>
        {
            await contributor.EmitRangeAsync(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        });

        var evens = await stream
            .Where(x => x % 2 == 0)
            .Select(x => x * 10)
            .ToListAsync();

        Assert.That(evens, Is.EqualTo(new[] { 20, 40, 60, 80, 100 }));
    }

    [Test]
    public async Task BoundedStream_AppliesBackpressure()
    {
        var options = new AsyncCollectionStreamOptions
        {
            BoundedCapacity = 2
        };

        var stream = new AsyncCollectionStream<int>(options);
        var contributor = stream.RegisterContributor("Producer");
        stream.Seal();

        var producedCount = 0;
        var produceTask = Task.Run(async () =>
        {
            for (int i = 1; i <= 5; i++)
            {
                await contributor.EmitAsync(i);
                Interlocked.Increment(ref producedCount);
            }
            contributor.Complete();
        });

        await Task.Delay(50);
        Assert.That(producedCount, Is.LessThanOrEqualTo(3));

        var consumed = new List<int>();
        await foreach (var item in stream)
        {
            consumed.Add(item);
        }

        await produceTask;
        Assert.That(consumed, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void NestedContributor_Fault_PropagatesExceptionToConsumer()
    {
        var stream = AsyncCollectionStream.Create<int>(async root =>
        {
            await root.EmitAsync(100);
            await root.ProduceNestedAsync(async child =>
            {
                await Task.Yield();
                throw new InvalidOperationException("Simulated error in nested contributor");
            });
        });

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in stream)
            {
            }
        });

        Assert.That(ex!.Message, Does.Contain("Simulated error in nested contributor"));
    }

    [Test]
    public async Task ManualRegisterAndSeal_ControlsLifecycle()
    {
        var stream = new AsyncCollectionStream<string>();

        var c1 = stream.RegisterContributor("C1");
        var c2 = stream.RegisterContributor("C2");
        stream.Seal();

        var consumerTask = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in stream)
            {
                items.Add(item);
            }
            return items;
        });

        await c1.EmitAsync("From C1");
        c1.Complete();

        Assert.That(stream.IsCompleted, Is.False);

        await c2.EmitAsync("From C2");
        c2.Complete();

        var results = await consumerTask;
        Assert.That(results, Is.EqualTo(new[] { "From C1", "From C2" }));
        Assert.That(stream.IsCompleted, Is.True);
    }
}

