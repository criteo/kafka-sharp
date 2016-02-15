using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Common;
using Kafka.Network;
using Moq;
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
            public MemoryStream Data;
        }

        [Test]
        public void TestCreation()
        {
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(() => new RealSocketAsyncEventArgs());
            var dummy = new Connection("localhost", 0, _ => socket.Object, 1234, 4321);

            socket.VerifySet(s => s.SendBufferSize = 1234);
            socket.VerifySet(s => s.ReceiveBufferSize = 4321);
        }

        [Test]
        public void TestDispose()
        {
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(() => new RealSocketAsyncEventArgs());
            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object);

            connection.Dispose();

            socket.Verify(s => s.Close(), Times.Once());
        }

        [Test]
        public void TestSendWithoutConnect()
        {
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(new RealSocketAsyncEventArgs());
            socket.Setup(s => s.Connected).Returns(false);
            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object);

            var ex = Assert.Throws<TransportException>(async () => await connection.SendAsync(0, ReusableMemoryStream.Reserve(), true));
            Assert.AreEqual(TransportError.ConnectError, ex.Error);
        }

        [Test]
        public async Task TestConnect()
        {
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.ConnectAsync()).Returns(Task.FromResult(true));
            socket.Setup(s => s.CreateEventArgs()).Returns(new Mock<ISocketAsyncEventArgs>().Object);
            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object);
            await connection.ConnectAsync();
            socket.Verify(s => s.ConnectAsync(), Times.Once());
            socket.Verify(s => s.ReceiveAsync(It.IsAny<ISocketAsyncEventArgs>()), Times.Once());
        }

        [Test]
        public async Task TestSendAsync()
        {
            var mocked = new Dictionary<ISocketAsyncEventArgs, Mock<ISocketAsyncEventArgs>>();
            var socket = new Mock<ISocket>();
            int step = 0; // we'll do 3 async steps
            socket.Setup(s => s.CreateEventArgs()).Returns(() =>
            {
                var saea = new Mock<ISocketAsyncEventArgs>();
                saea.Setup(a => a.SocketError).Returns(SocketError.Success);
                saea.SetupProperty(a => a.UserToken);
                saea.Setup(a => a.BytesTransferred).Returns(() =>
                {
                    switch (step)
                    {
                        case 1:
                            return 4;
                        case 2:
                            return 7;
                        default:
                            return 3;
                    }
                });
                saea.Setup(a => a.Count).Returns(() =>
                {
                    switch (step)
                    {
                        case 1:
                            return 14;
                        case 2:
                            return 10;
                        default:
                            return 3;
                    }
                });
                saea.Setup(a => a.Offset).Returns(() =>
                {
                    switch (step)
                    {
                        case 1:
                            return 0;
                        case 2:
                            return 4;
                        default:
                            return 11;
                    }
                });
                mocked.Add(saea.Object, saea);
                return saea.Object;
            });
            socket.Setup(s => s.Connected).Returns(true);
            SocketError error = SocketError.WouldBlock;
            var buffer = ReusableMemoryStream.Reserve(14);
            socket.Setup(
                s =>
                    s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error))
                .Returns(0);

            socket.Setup(s => s.SendAsync(It.IsAny<ISocketAsyncEventArgs>()))
                .Returns(() => ++step == 3) // simulate a synchronous return on step 1 and 2
                .Callback((ISocketAsyncEventArgs args) =>
                {
                    if (step == 3)
                        mocked[args].Raise(a => a.Completed += null, socket.Object, args);
                });

            var connection = new Connection(new IPEndPoint(0,0), _ => socket.Object);
            await connection.SendAsync(12, buffer, true);
            socket.Verify(s => s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error), Times.Once());
            socket.Verify(s => s.SendAsync(It.IsAny<ISocketAsyncEventArgs>()), Times.Exactly(3));
        }

        [Test]
        public async Task TestSendAsyncSmallInternalBuffer()
        {
            var mocked = new Dictionary<ISocketAsyncEventArgs, Mock<ISocketAsyncEventArgs>>();
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(() =>
            {
                var saea = new Mock<ISocketAsyncEventArgs>();
                int from = 0;
                int count = 0;
                saea.Setup(a => a.SetBuffer(It.IsAny<int>(), It.IsAny<int>())).Callback((int f, int c) =>
                {
                    from = f;
                    count = c;
                });
                saea.Setup(a => a.SocketError).Returns(SocketError.Success);
                saea.SetupProperty(a => a.UserToken);
                saea.Setup(a => a.BytesTransferred).Returns(() => count);
                saea.Setup(a => a.Count).Returns(() => count);
                saea.Setup(a => a.Offset).Returns(() => from);
                mocked.Add(saea.Object, saea);
                return saea.Object;
            });
            socket.Setup(s => s.Connected).Returns(true);
            SocketError error = SocketError.WouldBlock;
            var buffer = ReusableMemoryStream.Reserve(14);
            socket.Setup(
                s =>
                    s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error))
                .Returns(0);

            socket.Setup(s => s.SendAsync(It.IsAny<ISocketAsyncEventArgs>()))
                .Returns(true)
                .Callback((ISocketAsyncEventArgs args) =>
                {
                    mocked[args].Raise(a => a.Completed += null, socket.Object, args);
                });

            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object, 8, 8, true);
            await connection.SendAsync(12, buffer, true);

            // Ok, so the connection has an 8 bytes send buffer and the data to send is 14 bytes long,
            // so we should see two calls to socket.SendAsync since socket.Send is set to always return E_WOULDBLOCK

            socket.Verify(s => s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error), Times.Once());
            socket.Verify(s => s.SendAsync(It.IsAny<ISocketAsyncEventArgs>()), Times.Exactly(2));
        }

        [Test]
        public void TestSendAsyncErrorInSocketSendAsync()
        {
            var mocked = new Dictionary<ISocketAsyncEventArgs, Mock<ISocketAsyncEventArgs>>();
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(() =>
            {
                var saea = new Mock<ISocketAsyncEventArgs>();
                saea.Setup(a => a.SocketError).Returns(SocketError.SocketError);
                saea.SetupProperty(a => a.UserToken);
                mocked.Add(saea.Object, saea);
                return saea.Object;
            });
            socket.Setup(s => s.Connected).Returns(true);
            SocketError error = SocketError.WouldBlock;
            var buffer = ReusableMemoryStream.Reserve(14);
            socket.Setup(
                s =>
                    s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error))
                .Returns(0);
            socket.Setup(s => s.SendAsync(It.IsAny<ISocketAsyncEventArgs>()))
                .Returns(true)
                .Callback((ISocketAsyncEventArgs a) => mocked[a].Raise(_ => _.Completed += null, socket.Object, a));
            var connection = new Connection(new IPEndPoint(0,0), _ => socket.Object);
            var e = Assert.Throws<TransportException>(async () => await connection.SendAsync(12, buffer, true));
            Assert.That(e.Error, Is.EqualTo(TransportError.WriteError));
        }

        [Test]
        public void TestSendAsyncErrorInSocketSend()
        {
            var mocked = new Dictionary<ISocketAsyncEventArgs, Mock<ISocketAsyncEventArgs>>();
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(() => new Mock<ISocketAsyncEventArgs>().Object);
            socket.Setup(s => s.Connected).Returns(true);
            SocketError error = SocketError.NetworkReset;
            var buffer = ReusableMemoryStream.Reserve(14);
            socket.Setup(
                s =>
                    s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error))
                .Returns(0);
            socket.Setup(s => s.SendAsync(It.IsAny<ISocketAsyncEventArgs>()))
                .Returns(true)
                .Callback((ISocketAsyncEventArgs a) => mocked[a].Raise(_ => _.Completed += null, socket.Object, a));
            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object);
            var e = Assert.Throws<TransportException>(async () => await connection.SendAsync(12, buffer, true));
            Assert.That(e.Error, Is.EqualTo(TransportError.WriteError));
            Assert.IsInstanceOf<SocketException>(e.InnerException);
            var se = e.InnerException as SocketException;
            Assert.That(se.ErrorCode, Is.EqualTo((int) SocketError.NetworkReset));
        }

        [Test]
        public void TestSendAsyncErrorSocketSendThrow()
        {
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(() =>
            {
                var saea = new Mock<ISocketAsyncEventArgs>();
                return saea.Object;
            });
            socket.Setup(s => s.Connected).Returns(true);
            SocketError error = SocketError.WouldBlock;
            var buffer = ReusableMemoryStream.Reserve(14);
            socket.Setup(
                s =>
                    s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error))
                .Throws<InvalidOperationException>();
            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object);
            var e = Assert.Throws<TransportException>(async () => await connection.SendAsync(12, buffer, true));
            Assert.That(e.Error, Is.EqualTo(TransportError.WriteError));
            Assert.IsInstanceOf<InvalidOperationException>(e.InnerException);
        }

        [Test]
        public void TestSendAsyncErrorSocketSendAsyncThrow()
        {
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(() =>
            {
                var saea = new Mock<ISocketAsyncEventArgs>();
                return saea.Object;
            });
            socket.Setup(s => s.Connected).Returns(true);
            SocketError error = SocketError.WouldBlock;
            var buffer = ReusableMemoryStream.Reserve(14);
            socket.Setup(
                s =>
                    s.Send(It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<SocketFlags>(), out error))
                .Returns(0);
            socket.Setup(s => s.SendAsync(It.IsAny<ISocketAsyncEventArgs>()))
                .Throws<InvalidOperationException>();
            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object);
            var e = Assert.Throws<TransportException>(async () => await connection.SendAsync(12, buffer, true));
            Assert.That(e.Error, Is.EqualTo(TransportError.WriteError));
            Assert.IsInstanceOf<InvalidOperationException>(e.InnerException);
        }

        [Test]
        public async Task TestNormalFlow()
        {
            const string data = "The cuiqk brwon fox jumps over the zaly god.";

            var server = new FakeServer(SimulationMode.Success);
            var connection = new Connection(server.EndPoint, Connection.DefaultSocketFactory, 16, 16);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = ReusableMemoryStream.Reserve();
            BigEndianConverter.Write(buffer, 4 + 4 + 1 + data.Length - 4);
            BigEndianConverter.Write(buffer, correlationId);
            buffer.WriteByte(ack);
            var s = Encoding.UTF8.GetBytes(data);
            buffer.Write(s, 0, s.Length);

            var p = new TaskCompletionSource<Response>();
            connection.Response +=
                (con, cor, b) => p.SetResult(new Response {Connection = con, CorrelationId = cor, Data = b});

            await connection.ConnectAsync();
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();

            Assert.AreSame(connection, r.Connection);
            Assert.AreEqual(correlationId, r.CorrelationId);
            Assert.AreEqual(data, Encoding.UTF8.GetString(r.Data.ToArray()));
        }

        [Test]
        public async Task TestNoAckFlow()
        {
            const string data = "The cuiqk brwon fox jumps over the zaly god.";

            var server = new FakeServer(SimulationMode.Success);
            var connection = new Connection(server.EndPoint, Connection.DefaultSocketFactory, 16, 16);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = ReusableMemoryStream.Reserve();
            BigEndianConverter.Write(buffer, 4 + 4 + 1 + data.Length - 4);
            BigEndianConverter.Write(buffer, correlationId);
            buffer.WriteByte(ack);
            var s = Encoding.UTF8.GetBytes(data);
            buffer.Write(s, 0, s.Length);

            var bdata = buffer.ToArray();

            var p = new TaskCompletionSource<Response>();
            connection.Response +=
                (con, cor, b) => p.SetResult(new Response { Connection = con, CorrelationId = cor, Data = b });

            await connection.ConnectAsync();
            buffer.Position = 8;
            buffer.WriteByte(0);
            await connection.SendAsync(correlationId, buffer, false);
            buffer = ReusableMemoryStream.Reserve();
            buffer.Write(bdata, 0, bdata.Length);
            buffer.Position = 8;
            buffer.WriteByte(1);
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();

            Assert.AreSame(connection, r.Connection);
            Assert.AreEqual(correlationId, r.CorrelationId);
            Assert.AreEqual(data, Encoding.UTF8.GetString(r.Data.ToArray()));
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
            var connection = new Connection(server.EndPoint, Connection.DefaultSocketFactory);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = ReusableMemoryStream.Reserve();
            BigEndianConverter.Write(buffer, 4 + 4 + 1 + data.Length - 4);
            BigEndianConverter.Write(buffer, correlationId);
            buffer.WriteByte(ack);
            var s = Encoding.UTF8.GetBytes(data);
            buffer.Write(s, 0, s.Length);

            var p = new TaskCompletionSource<Error>();
            connection.ReceiveError += (c, e) => p.TrySetResult(new Error {Connection = c, Exception = e});

            await connection.ConnectAsync();
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();

            Assert.AreSame(connection, r.Connection);
            Assert.IsInstanceOf<TransportException>(r.Exception);
            var exception = r.Exception as TransportException;
            Assert.That(exception.Error, Is.EqualTo(TransportError.ReadError));
        }

        [Test]
        public async Task TestCorrelationError()
        {
            const string data = "The cuiqk brwon fox jumps over the zaly god.";

            var server = new FakeServer(SimulationMode.CorrelationIdError);
            var connection = new Connection(server.EndPoint, Connection.DefaultSocketFactory);
            const int correlationId = 379821;
            const byte ack = 1;
            var buffer = ReusableMemoryStream.Reserve();
            BigEndianConverter.Write(buffer, 4 + 4 + 1 + data.Length - 4);
            BigEndianConverter.Write(buffer, correlationId);
            buffer.WriteByte(ack);
            var s = Encoding.UTF8.GetBytes(data);
            buffer.Write(s, 0, s.Length);

            var p = new TaskCompletionSource<Error>();
            connection.ReceiveError += (c, e) => p.TrySetResult(new Error { Connection = c, Exception = e });

            await connection.ConnectAsync();
            await connection.SendAsync(correlationId, buffer, true);
            var r = await p.Task;

            server.Stop();
            connection.Dispose();

            Assert.AreSame(connection, r.Connection);
            Assert.IsInstanceOf<CorrelationException>(r.Exception);
            var exception = r.Exception as CorrelationException;
            Assert.AreEqual(correlationId, exception.Expected);
        }

        [Test]
        public void TestConnectionErrorThrowsTransportException()
        {
            var socket = new Mock<ISocket>();
            socket.Setup(s => s.CreateEventArgs()).Returns(new RealSocketAsyncEventArgs());
            socket.Setup(s => s.ConnectAsync()).Throws(new SocketException((int) SocketError.ConnectionRefused));
            var connection = new Connection(new IPEndPoint(0, 0), _ => socket.Object);

            var exception = Assert.Throws<TransportException>(async () => await connection.ConnectAsync());
            Assert.That(exception.Error, Is.EqualTo(TransportError.ConnectError));
        }
    }
}
