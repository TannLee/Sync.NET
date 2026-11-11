using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace SyncDotNet.KineDB;

internal sealed class WorkerProcess : IAsyncDisposable
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly Process _process;
    private readonly Task _reader;
    private readonly Task _diagnostics;
    private readonly SemaphoreSlim _writer = new(1);
    private readonly ConcurrentDictionary<long, TaskCompletionSource<JsonElement>> _pending = new();
    private long _nextId;
    private int _closed;
    private int _disposed;

    internal WorkerProcess(ProcessStartInfo start)
    {
        start.RedirectStandardInput = true;
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        start.StandardInputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        start.StandardOutputEncoding = Encoding.UTF8;
        _process = Process.Start(start) ?? throw new IOException("Could not start the KineDB worker.");
        _diagnostics = _process.StandardError.BaseStream.CopyToAsync(Stream.Null);
        _reader = ReadResponsesAsync();
    }

    internal bool Closed => Volatile.Read(ref _closed) != 0;

    internal async Task<JsonElement> SendAsync(
        string command, string? key, string? payload, string? password,
        bool interruptible, CancellationToken cancellationToken)
    {
        var id = Interlocked.Increment(ref _nextId);
        var pending = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        await _writer.WaitAsync(cancellationToken);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Closed)
                throw new IOException("The KineDB worker is unavailable.");

            _pending[id] = pending;
            if (Closed)
            {
                _pending.TryRemove(id, out _);
                throw new IOException("The KineDB worker is unavailable.");
            }

            var line = JsonSerializer.Serialize(new { id, command, key, payload, password }, Json);
            try
            {
                await _process.StandardInput.WriteLineAsync(line);
                await _process.StandardInput.FlushAsync();
            }
            catch
            {
                _pending.TryRemove(id, out _);
                throw;
            }
        }
        finally
        {
            _writer.Release();
        }

        try
        {
            return interruptible ? await pending.Task.WaitAsync(cancellationToken) : await pending.Task;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await AbortAsync();
            throw;
        }
    }

    private async Task ReadResponsesAsync()
    {
        try
        {
            while (await _process.StandardOutput.ReadLineAsync() is { } line)
            {
                using var document = JsonDocument.Parse(line);
                var response = document.RootElement;
                var id = response.GetProperty("id").GetInt64();
                if (response.TryGetProperty("error", out var error))
                {
                    var failure = new KineDbOperationException(
                        error.GetString() ?? "KineDB operation failed.",
                        response.GetProperty("retryable").GetBoolean(),
                        response.GetProperty("outcomeUnknown").GetBoolean());
                    if (_pending.TryRemove(id, out var pending))
                        pending.TrySetException(failure);
                }
                else
                {
                    var value = response.GetProperty("value").Clone();
                    if (_pending.TryRemove(id, out var pending))
                        pending.TrySetResult(value);
                }
            }
        }
        catch (Exception)
        {
            // A malformed response means the process can no longer be used.
            Kill();
        }
        finally
        {
            Interlocked.Exchange(ref _closed, 1);
            foreach (var (id, _) in _pending)
            {
                if (_pending.TryRemove(id, out var pending))
                    pending.TrySetException(new IOException("The KineDB worker ended before replying."));
            }
        }
    }

    private void Kill()
    {
        try
        {
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException) { }
    }

    private async Task AbortAsync()
    {
        Kill();
        await _reader;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _writer.WaitAsync();
        try
        {
            _process.StandardInput.Close();
        }
        finally
        {
            _writer.Release();
        }

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            Kill();
            await _process.WaitForExitAsync();
        }
        await Task.WhenAll(_reader, _diagnostics);
        _process.Dispose();
    }
}

internal sealed class KineDbOperationException(string message, bool retryable, bool outcomeUnknown)
    : IOException(message)
{
    internal bool Retryable { get; } = retryable;
    internal bool OutcomeUnknown { get; } = outcomeUnknown;
}
