using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using SyncDotNet.KineDB;

namespace SyncDotNet.IntegrationTests;

public sealed class Phase1Tests
{
    [KineDbServerFact]
    public async Task Two_replicas_converge_after_offline_edits_and_reopen()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var server = await ProbeServer.StartAsync(token);
        var mapping = new CollectionMapping<Note>("phase1_notes", "note-v1", note => note.Id);
        await server.SqlAsync(KineDbCollection<Note>.CreateTableSql(mapping.Name), token);

        await using var b = server.Open(mapping, "b");
        await using (var a = server.Open(mapping, "a"))
        {
            await Assert.ThrowsAnyAsync<IOException>(() => a.PutAsync(new("early", "must not be lost"), token));
            var initial = await a.SynchronizeAsync(cancellationToken: token);
            Assert.True(initial.Status == SyncStatus.UpToDate, initial.ToString());
            Assert.Equal(SyncStatus.UpToDate, (await b.SynchronizeAsync(cancellationToken: token)).Status);
            await a.PutAsync(new("modify", "old"), token);
            await a.PutAsync(new("delete", "old"), token);
            Assert.Equal(2, (await a.SynchronizeAsync(cancellationToken: token)).Push!.Sent);
            Assert.Equal(SyncStatus.UpToDate, (await b.SynchronizeAsync(cancellationToken: token)).Status);
        }

        // A reopened replica only contacts the server when SynchronizeAsync is called.
        await using (var a = server.Open(mapping, "a", _ => throw new InvalidOperationException("Offline operation requested credentials.")))
        {
            await a.PutAsync(new("modify", "updated ' Ω"), token);
            Assert.True(await a.DeleteAsync("delete", token));
            Assert.False(await a.DeleteAsync("missing", token));
            await a.PutAsync(new("new", "inserted"), token);
            Assert.Equal(new Note("modify", "updated ' Ω"), await a.GetAsync("modify", token));
            Assert.Null(await a.GetAsync("delete", token));
            Assert.Equal(new Note("modify", "old"), await b.GetAsync("modify", token));
        }

        await using var restarted = server.Open(mapping, "a");
        var stages = new List<SyncStage>();
        var pushed = await restarted.SynchronizeAsync(new InlineProgress(stages.Add), token);
        Assert.Equal(SyncStatus.UpToDate, pushed.Status);
        Assert.Equal(3, pushed.Push!.Sent);
        Assert.All(pushed.Push.Outcomes, outcome => Assert.Equal(WriteStatus.Applied, outcome.Status));
        Assert.Equal([SyncStage.CheckingServer, SyncStage.Pushing, SyncStage.Pulling, SyncStage.Completed], stages);
        var pulled = await b.SynchronizeAsync(cancellationToken: token);
        Assert.Equal(SyncStatus.UpToDate, pulled.Status);
        Assert.False(pulled.Pull!.Full);
        Assert.False(string.IsNullOrEmpty(pulled.Pull.ResumedFrom));
        Assert.Equal(2, pulled.Pull.Upserts);
        Assert.Equal(1, pulled.Pull.Removes);
        var expected = new[] { new Note("modify", "updated ' Ω"), new Note("new", "inserted") };
        Assert.Equal(expected, await restarted.ListAsync(token));
        Assert.Equal(expected, await b.ListAsync(token));
        Assert.Equal(SyncStatus.Cancelled, (await restarted.SynchronizeAsync(cancellationToken: new(true))).Status);

        await restarted.PutAsync(new("modify", "winner"), token);
        await b.PutAsync(new("modify", "stale edit"), token);
        Assert.Equal(SyncStatus.UpToDate, (await restarted.SynchronizeAsync(cancellationToken: token)).Status);
        var conflict = await b.SynchronizeAsync(cancellationToken: token);
        Assert.Equal(SyncStatus.Partial, conflict.Status);
        Assert.Equal(WriteStatus.Conflict, Assert.Single(conflict.Push!.Outcomes).Status);
        Assert.False(conflict.PushOutcomeUnknown);
    }

    [KineDbServerFact]
    public async Task An_edit_requested_during_push_commits_after_the_earlier_acknowledgment()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var token = timeout.Token;
        await using var server = await ProbeServer.StartAsync(token);
        await using var proxy = new ReplyGate(server.Port);
        var mapping = new CollectionMapping<Note>("phase1_notes", "note-v1", note => note.Id);
        await server.SqlAsync(KineDbCollection<Note>.CreateTableSql(mapping.Name), token);

        await using var replica = server.Open(mapping, "a", endpoint: proxy.Endpoint);
        var initial = await replica.SynchronizeAsync(cancellationToken: token);
        Assert.True(initial.Status == SyncStatus.UpToDate, initial.ToString());
        await replica.PutAsync(new("key", "first"), token);
        var push = replica.SynchronizeAsync(new InlineProgress(stage =>
        {
            if (stage == SyncStage.Pushing) proxy.HoldReplies();
        }), token);

        Task? queuedEdit = null;
        try
        {
            await WaitForRemoteNote(server, "first", token);
            Assert.False(push.IsCompleted);
            queuedEdit = replica.PutAsync(new("key", "second"), token);
            Assert.False(queuedEdit.IsCompleted);
            Assert.Equal(new Note("key", "first"), await replica.GetAsync("key", token));
        }
        finally
        {
            proxy.ReleaseReplies();
        }

        var earlier = await push;
        Assert.Equal(1, earlier.Push!.Sent);
        await queuedEdit!;
        Assert.Equal(new Note("key", "second"), await replica.GetAsync("key", token));
        await replica.DisposeAsync();
        await using var reopened = server.Open(mapping, "a", endpoint: proxy.Endpoint);
        var later = await reopened.SynchronizeAsync(cancellationToken: token);
        Assert.True(later.Status == SyncStatus.UpToDate, later.ToString());
        Assert.Equal(1, later.Push!.Sent);
        await WaitForRemoteNote(server, "second", token);

        await reopened.PutAsync(new("key", "third"), token);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(token);
        var interrupted = reopened.SynchronizeAsync(new InlineProgress(stage =>
        {
            if (stage == SyncStage.Pushing) proxy.HoldReplies();
        }), cancel.Token);
        try
        {
            await WaitForRemoteNote(server, "third", token);
            Assert.False(interrupted.IsCompleted);
            await cancel.CancelAsync();
            var cancelled = await interrupted;
            Assert.Equal(SyncStatus.Cancelled, cancelled.Status);
            Assert.True(cancelled.PushOutcomeUnknown);
        }
        finally
        {
            proxy.ReleaseReplies();
        }

        var replay = await reopened.SynchronizeAsync(cancellationToken: token);
        Assert.True(replay.Status == SyncStatus.UpToDate, replay.ToString());
        Assert.True(Assert.Single(replay.Push!.Outcomes).Replayed);
        Assert.Equal(new Note("key", "third"), await reopened.GetAsync("key", token));
        Assert.Equal(0, (await reopened.SynchronizeAsync(cancellationToken: token)).Push!.Sent);
    }

    private static async Task WaitForRemoteNote(ProbeServer server, string text, CancellationToken token)
    {
        while (true)
        {
            var response = await server.SqlAsync("SELECT payload FROM phase1_notes WHERE id = 'key'", token);
            var rows = response.GetProperty("data");
            if (rows.GetArrayLength() == 1 && JsonSerializer.Deserialize<Note>(rows[0][0].GetString()!)?.Text == text)
                return;
            await Task.Delay(25, token);
        }
    }

    private sealed record Note(string Id, string Text);
    private sealed class InlineProgress(Action<SyncStage> report) : IProgress<SyncStage>
    {
        public void Report(SyncStage stage) => report(stage);
    }

    private sealed class ProbeServer : IAsyncDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("syncnet-phase1-");
        private readonly string _password = Guid.NewGuid().ToString("N");
        private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(5) };
        private Process? _process;
        internal int Port => _http.BaseAddress!.Port;

        internal static async Task<ProbeServer> StartAsync(CancellationToken token)
        {
            var server = new ProbeServer();
            try
            {
                await server.StartProcessAsync(token);
                return server;
            }
            catch
            {
                await server.DisposeAsync();
                throw;
            }
        }

        private async Task StartProcessAsync(CancellationToken token)
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            _http.BaseAddress = new Uri($"http://127.0.0.1:{port}");

            var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("SYNCNET_KINEDB_SERVER_EXE")!)
            {
                UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true
            };
            foreach (var arg in new[] { "--listen", "127.0.0.1", "--port", port.ToString(), "--data-dir",
                Path.Combine(_directory.FullName, "server"), "--no-web" })
                start.ArgumentList.Add(arg);
            start.Environment["KINEDB_ROOT_PASSWORD"] = _password;
            _process = Process.Start(start) ?? throw new IOException("Could not start the test server.");
            _ = _process.StandardOutput.BaseStream.CopyToAsync(Stream.Null, token);
            _ = _process.StandardError.BaseStream.CopyToAsync(Stream.Null, token);

            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < deadline)
            {
                token.ThrowIfCancellationRequested();
                if (_process.HasExited) throw new IOException("The KineDB test server exited.");
                try
                {
                    using var health = await _http.GetAsync("/health", token);
                    if (health.IsSuccessStatusCode)
                    {
                        using var body = new StringContent(JsonSerializer.Serialize(new { username = "root", password = _password }),
                            Encoding.UTF8, "application/json");
                        using var login = await _http.PostAsync("/login", body, token);
                        if (login.IsSuccessStatusCode)
                        {
                            using var json = JsonDocument.Parse(await login.Content.ReadAsStringAsync(token));
                            _http.DefaultRequestHeaders.Authorization = new("Bearer", json.RootElement.GetProperty("token").GetString());
                            return;
                        }
                    }
                }
                catch (HttpRequestException) { }
                await Task.Delay(100, token);
            }
            throw new IOException("The KineDB test server did not become ready for login.");
        }

        internal KineDbCollection<T> Open<T>(CollectionMapping<T> mapping, string name,
            Func<CancellationToken, ValueTask<string>>? password = null, Uri? endpoint = null) where T : class =>
            new(mapping, new(Environment.GetEnvironmentVariable("SYNCNET_KINEDB_WASM_PACKAGE")!,
                Path.Combine(_directory.FullName, name),
                endpoint ?? new UriBuilder(_http.BaseAddress!) { Scheme = "ws", Path = "/ws" }.Uri,
                "root", password ?? (_ => ValueTask.FromResult(_password))));

        internal async Task<JsonElement> SqlAsync(string sql, CancellationToken token)
        {
            using var response = await _http.PostAsync("/sql", new StringContent(sql, Encoding.UTF8), token);
            response.EnsureSuccessStatusCode();
            using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(token));
            Assert.NotEqual("error", json.RootElement.GetProperty("type").GetString());
            return json.RootElement.Clone();
        }

        public async ValueTask DisposeAsync()
        {
            _http.Dispose();
            if (_process is not null)
            {
                if (!_process.HasExited) _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync();
                _process.Dispose();
            }
            _directory.Delete(recursive: true);
        }
    }

    // Pause only the server-to-client direction so the server can commit a push
    // while the caller can still edit the local replica before the ACK arrives.
    private sealed class ReplyGate : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly List<Task> _connections = [];
        private readonly int _upstreamPort;
        private readonly Task _accept;
        private TaskCompletionSource? _gate;

        internal ReplyGate(int upstreamPort)
        {
            _upstreamPort = upstreamPort;
            _listener.Start();
            _accept = AcceptAsync();
        }

        internal Uri Endpoint => new($"ws://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/ws");
        internal void HoldReplies() => Volatile.Write(ref _gate, new(TaskCreationOptions.RunContinuationsAsynchronously));
        internal void ReleaseReplies() => Interlocked.Exchange(ref _gate, null)?.TrySetResult();

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    _connections.Add(ForwardAsync(client));
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        }

        private async Task ForwardAsync(TcpClient client)
        {
            using (client)
            using (var upstream = new TcpClient())
            using (var stop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token))
            {
                try
                {
                    await upstream.ConnectAsync(IPAddress.Loopback, _upstreamPort, stop.Token);
                    using var fromClient = client.GetStream();
                    using var fromServer = upstream.GetStream();
                    var request = fromClient.CopyToAsync(fromServer, stop.Token);
                    var reply = ForwardRepliesAsync(fromServer, fromClient, stop.Token);
                    await Task.WhenAny(request, reply);
                    await stop.CancelAsync();
                    await Task.WhenAll(request, reply);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
                catch (IOException) { }
            }
        }

        private async Task ForwardRepliesAsync(Stream source, Stream target, CancellationToken token)
        {
            var buffer = new byte[16_384];
            int count;
            while ((count = await source.ReadAsync(buffer, token)) != 0)
            {
                if (Volatile.Read(ref _gate) is { } gate) await gate.Task.WaitAsync(token);
                await target.WriteAsync(buffer.AsMemory(0, count), token);
            }
        }

        public async ValueTask DisposeAsync()
        {
            ReleaseReplies();
            await _stop.CancelAsync();
            await _accept;
            _listener.Stop();
            await Task.WhenAll(_connections);
            _stop.Dispose();
        }
    }
}

public sealed class KineDbServerFactAttribute : FactAttribute
{
    public KineDbServerFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SYNCNET_KINEDB_WASM_PACKAGE")) ||
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SYNCNET_KINEDB_SERVER_EXE")))
        {
            Skip = "BLOCKED: set SYNCNET_KINEDB_WASM_PACKAGE and SYNCNET_KINEDB_SERVER_EXE for the Windows integration probe.";
        }
    }
}
