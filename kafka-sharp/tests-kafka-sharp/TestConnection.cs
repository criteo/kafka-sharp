using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Cluster;
using Kafka.Common;
using Kafka.Network;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestConnection
    {
        enum SimulationMode
        {
            Success,
            ConnectError,
            SendError,
            ReceiveError,
            CorrelationIdError
        }

        /// <summary>
        /// This server does not implement Kafka protocol,
        /// only the part understood by Connection which is only
        /// decoding the size and correlation id in the response.
        /// 
        /// Expected input is:
        /// size(4b) - correlationid(4b) - ack(1b) - data
        /// Response will be:
        /// size(4b) - correlationid(4b) - data
        /// 
        /// sise doe not include the 4 bytes of size.
        /// </summary>
        class FakeServer
        {
            private readonly TcpListener _listener;
            private readonly CancellationTokenSource _cancel = new CancellationTokenSource();
            private readonly Task _loop;
            private SimulationMode _mode;

            public FakeServer(SimulationMode mode)
            {
                _listener = new TcpListener(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 0));
                _listener.Start();
                if (mode != SimulationMode.ConnectError)
                {
                    _loop = ListenLoop();
                }
                _mode = mode;
            }

            public EndPoint EndPoint { get { return _listener.LocalEndpoint; } }

            public void Stop()
            {
                _cancel.Cancel();
                try
                {
                    _listener.Stop();
                }
                catch
                {
                }
            }

            private async Task ListenLoop()
            {
                TcpClient client = null;
                try
                {
                    client = await _listener.AcceptTcpClientAsync();
                    while (!_cancel.IsCancellationRequested)
                    {
                        if (_mode == SimulationMode.SendError)
                        {
                            throw new SocketException((int)SocketError.ConnectionAborted);
                        }

                        var stream = client.GetStream();
                        var buffer = new byte[4];
                        int read = 0;
                        while (read != buffer.Length)
                            read += await stream.ReadAsync(buffer, read, buffer.Length - read, _cancel.Token).ConfigureAwait(false);
                        if (_mode == SimulationMode.ReceiveError)
                        {
                            throw new SocketException((int)SocketError.ConnectionAborted);
                        }
                        int size = BigEndianConverter.ToInt32(buffer);
                        buffer = new byte[size];
                        read = 0;
                        while (read != buffer.Length)
                            read += await stream.ReadAsync(buffer, read, buffer.Length - read, _cancel.Token).ConfigureAwait(false);
                        if (buffer[4] == 0)
                        {
                            continue;
                        }

                        var sbuffer = new byte[4 + size - 1];
                        int correlation = BigEndianConverter.ToInt32(buffer);
                        if (_mode == SimulationMode.CorrelationIdError)
                            correlation -= 723;

                        BigEndianConverter.Write(sbuffer, size - 1);
                        BigEndianConverter.Write(sbuffer, correlation, 4);
                        Array.Copy(buffer, 5, sbuffer, 8, buffer.Length - 5);
                        await stream.WriteAsync(sbuffer, 0, sbuffer.Length, _cancel.Token).ConfigureAwait(false);
                    }
                    client.Close();
                }
                catch
                {
                    if (client != null)
                        client.Close();
                    _listener.Stop();
                }
            }
        }

        class Response
        {
            public IConnection Connection;
            public int CorrelationId;
            public byte[] Data;
        }

        [Test]
        public async Task TestSendWithoutConnect()
        {
            var server = new FakeServer(SimulationMode.Success);
            var connection = new Connection(server.EndPoint);

            var ex = Assert.Throws<TransportException>(async () => await connection.SendAsync(0, new byte[9], true));
            Assert.AreEqual(TransportError.ConnectError, ex.Error);
        }

        [Test]
        public async Task TestNormalFlow()
        {
            const string data = "The cuiqk brwon fox jumps over the zaly god.";

            var server = new FakeServer(SimulationMode.Success);
            var connection = new Connection(server.EndPoint);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = new byte[4 + 4 + 1 + data.Length];
            BigEndianConverter.Write(buffer, buffer.Length - 4);
            BigEndianConverter.Write(buffer, correlationId, 4);
            buffer[8] = ack;
            Encoding.UTF8.GetBytes(data, 0, data.Length, buffer, 9);

            var p = new TaskCompletionSource<Response>();
            connection.Response +=
                (con, cor, b) => p.SetResult(new Response {Connection = con, CorrelationId = cor, Data = b});

            await connection.ConnectAsync();
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();

            Assert.AreSame(connection, r.Connection);
            Assert.AreEqual(correlationId, r.CorrelationId);
            Assert.AreEqual(data, Encoding.UTF8.GetString(r.Data));
        }

        [Test]
        public async Task TestNoAckFlow()
        {
            const string data = "The cuiqk brwon fox jumps over the zaly god.";

            var server = new FakeServer(SimulationMode.Success);
            var connection = new Connection(server.EndPoint);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = new byte[4 + 4 + 1 + data.Length];
            BigEndianConverter.Write(buffer, buffer.Length - 4);
            BigEndianConverter.Write(buffer, correlationId, 4);
            buffer[8] = ack;
            Encoding.UTF8.GetBytes(data, 0, data.Length, buffer, 9);

            var p = new TaskCompletionSource<Response>();
            connection.Response +=
                (con, cor, b) => p.SetResult(new Response { Connection = con, CorrelationId = cor, Data = b });

            await connection.ConnectAsync();
            buffer[8] = 0;
            await connection.SendAsync(correlationId, buffer, true);
            buffer[8] = 1;
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();

            Assert.AreSame(connection, r.Connection);
            Assert.AreEqual(correlationId, r.CorrelationId);
            Assert.AreEqual(data, Encoding.UTF8.GetString(r.Data));
        }

        class Error
        {
            public IConnection Connection;
            public Exception Exception;
        }

        [Test]
        public async Task TestReceiveError()
        {
            const string data = "The cuiqk brwon fox jumps over the zaly god.";

            var server = new FakeServer(SimulationMode.ReceiveError);
            var connection = new Connection(server.EndPoint);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = new byte[4 + 4 + 1 + data.Length];
            BigEndianConverter.Write(buffer, buffer.Length - 4);
            BigEndianConverter.Write(buffer, correlationId, 4);
            buffer[8] = ack;
            Encoding.UTF8.GetBytes(data, 0, data.Length, buffer, 9);

            var p = new TaskCompletionSource<Error>();
            connection.ReceiveError += (c, e) => p.TrySetResult(new Error {Connection = c, Exception = e});

            await connection.ConnectAsync();
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();

            Assert.AreSame(connection, r.Connection);
            Assert.IsInstanceOf<TransportException>(r.Exception);
        }

        [Test]
        public async Task TestCorrelationError()
        {
            const string data = "The cuiqk brwon fox jumps over the zaly god.";

            var server = new FakeServer(SimulationMode.CorrelationIdError);
            var connection = new Connection(server.EndPoint);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = new byte[4 + 4 + 1 + data.Length];
            BigEndianConverter.Write(buffer, buffer.Length - 4);
            BigEndianConverter.Write(buffer, correlationId, 4);
            buffer[8] = ack;
            Encoding.UTF8.GetBytes(data, 0, data.Length, buffer, 9);

            var p = new TaskCompletionSource<Error>();
            connection.ReceiveError += (c, e) => p.TrySetResult(new Error { Connection = c, Exception = e });

            await connection.ConnectAsync();
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();

            Assert.AreSame(connection, r.Connection);
            Assert.IsInstanceOf<CorrelationException>(r.Exception);
            Assert.AreEqual(correlationId, (r.Exception as CorrelationException).Expected);
        }

        [Test]
        public async Task TestConnectionErrorThrowsTransportException()
        {
            var connection = new Connection(new IPEndPoint(new IPAddress(0), 17));
            try
            {
                await connection.ConnectAsync();
                Assert.IsFalse(true);
            }
            catch (Exception ex)
            {
                Assert.IsInstanceOf<TransportException>(ex);
                Assert.AreEqual(TransportError.ConnectError, (ex as TransportException).Error);
            }
        }
    }
}
