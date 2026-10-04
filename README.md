# 🪺 StreamNest

**StreamNest** is a lightweight, high-performance asynchronous stream collection library for .NET with **zero external dependencies**. It enables multiple concurrent producers ("contributors") to push data into a single `IAsyncEnumerable<T>` stream and allows contributors to hierarchically spawn **nested child contributors** to any depth.

The stream remains open and alive until **all** contributors (roots, children, and descendants) finish, and then automatically seals and completes cleanly.

---

## ✨ Features

- **Nested Multiple Contributors**: Any contributor can spawn child contributors (`CreateNestedContributor` or `ProduceNestedAsync`).
- **Automatic Lifecycle Tracking**: Uses lock-free atomic reference counting across the hierarchy.
- **Zero Dependencies**: Pure .NET standard library implementation with zero third-party packages on modern .NET.
- **Multi-Targeting Support**: Works on **.NET Standard 2.0**, **.NET Standard 2.1**, and **.NET 10.0** (and .NET 6/8/9).
- **Native Async Enumeration**: Implements standard `IAsyncEnumerable<T>`, compatible with native `await foreach` and `System.Linq.AsyncEnumerable`.
- **Parallel Processing**: Built-in `ParallelForEachAsync` and `ForEachAsync` extension methods.
- **Backpressure & Flow Control**: Supports bounded channels with customizable full-modes (`Wait`, `DropOldest`, etc.).

---

## 🚀 Quick Start

### 1. Basic Nested Contributors

```csharp
using StreamNest;

// Create the stream and launch the root contributor
var stream = AsyncCollectionStream.Create<string>(async root =>
{
    await root.EmitAsync("Root: Started processing");

    // Spawn a child contributor running in the background
    var child = root.CreateNestedContributor("ChildWorker");
    _ = Task.Run(async () =>
    {
        try
        {
            await child.EmitAsync("Child: Fetching data...");

            // Child spawns a grandchild contributor
            await child.ProduceNestedAsync(async grandchild =>
            {
                await Task.Delay(50);
                await grandchild.EmitAsync("Grandchild: Deep nested item completed");
            });

            await child.EmitAsync("Child: Work finished");
        }
        finally
        {
            child.Complete();
        }
    });

    await root.EmitAsync("Root: Waiting for background workers");
});

// Consume items as they arrive in real time:
await foreach (var item in stream)
{
    Console.WriteLine(item);
}
```

---

### 2. Parallel Processing with `ParallelForEachAsync`

```csharp
using StreamNest;

await stream.ParallelForEachAsync(async item =>
{
    await ProcessHeavyWorkloadAsync(item);
}, maxDegreeOfParallelism: 4);
```

---

### 3. Backpressure & Bounded Buffering

```csharp
using StreamNest;

var options = new AsyncCollectionStreamOptions
{
    BoundedCapacity = 100, // Maximum items buffered
    FullMode = BoundedChannelFullMode.Wait // Throttles producers when full
};

var stream = new AsyncCollectionStream<DataPacket>(options);
```

---

## 📄 License
MIT License.
