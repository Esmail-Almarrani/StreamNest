namespace StreamNest;

using System;
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
#if NET6_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

/// <summary>
/// Helper class for validating arguments and throwing standard exceptions across multiple .NET target frameworks
/// (such as netstandard2.0, netstandard2.1, and net10.0).
/// </summary>
public static class ExceptionHelper
{
    /// <summary>
    /// Throws an <see cref="ArgumentNullException"/> if <paramref name="argument"/> is null.
    /// Emulates ArgumentNullException.ThrowIfNull across all target frameworks.
    /// </summary>
    /// <param name="argument">The reference type argument to validate as non-null.</param>
    /// <param name="paramName">The name of the parameter with which <paramref name="argument"/> corresponds.</param>
    public static void ThrowIfNull(
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        [NotNull]
#endif
        object? argument,
#if NET6_0_OR_GREATER
        [CallerArgumentExpression(nameof(argument))]
#endif
        string? paramName = null)
    {
        if (argument is null)
        {
            throw new ArgumentNullException(paramName ?? nameof(argument));
        }
    }

    /// <summary>
    /// Throws an <see cref="ArgumentException"/> if <paramref name="argument"/> is null, empty, or whitespace.
    /// </summary>
    public static void ThrowIfNullOrWhiteSpace(
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        [NotNull]
#endif
        string? argument,
#if NET8_0_OR_GREATER
        [CallerArgumentExpression(nameof(argument))]
#endif
        string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(argument))
        {
            throw new ArgumentException("Value cannot be null or whitespace.", paramName ?? nameof(argument));
        }
    }

    /// <summary>
    /// Throws an <see cref="InvalidOperationException"/> with the specified message.
    /// </summary>
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
    [DoesNotReturn]
#endif
    public static void ThrowInvalidOperation(string message)
    {
        throw new InvalidOperationException(message);
    }

    /// <summary>
    /// Throws an <see cref="ObjectDisposedException"/> for the specified object type name.
    /// </summary>
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
    [DoesNotReturn]
#endif
    public static void ThrowObjectDisposed(string objectName)
    {
        throw new ObjectDisposedException(objectName);
    }
}

