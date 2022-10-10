namespace Multithreading.Utils;

public sealed class BusMessageWriter : IAsyncDisposable
{
    private readonly IBusConnection _connection;
    
    private readonly MemoryStream _buffer = new();
    
    private readonly Semaphore _pool = new(1, 1);

    public BusMessageWriter(IBusConnection connection)
    {
        _connection = connection;
    }
    
    public async Task SendMessageAsync(byte[] nextMessage)
    {
        _pool.WaitOne();
        
        _buffer.Write(nextMessage, 0, nextMessage.Length);

        if (_buffer.Length > 1000)
        {
            await _connection.PublishAsync(_buffer.ToArray());
            _buffer.SetLength(0);
        }

        _pool.Release();
    }

    public async ValueTask DisposeAsync()
    {
        if (_buffer.Length > 0)
        {
            await _connection.PublishAsync(_buffer.ToArray());
        }
        
        await _buffer.DisposeAsync();
        _pool.Dispose();
        
        GC.SuppressFinalize(this);
    }
}
