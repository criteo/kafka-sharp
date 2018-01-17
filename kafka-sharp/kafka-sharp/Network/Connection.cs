// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Kafka.Common;
using Kafka.Public;
#if NET_CORE
using System.Threading;
#endif

namespace Kafka.Network
{
    /// <summary>
    /// Thrown when correlation mismatch occurs
    /// </summary>
    class CorrelationException : Exception
    {
        public CorrelationException(int expected, int received)
        {
            Expected = expected;
            Received = received;
        }

        public int Expected { get; private set; }
        public int Received { get; private set; }

        public override string Message
        {
            get { return "Expected correlation id was: " + Expected + " but received " + Received + " instead."; }
        }
    }

    /// <summary>
    /// Flag the transport errors
    /// </summary>
    enum TransportError
    {
        ConnectError,
        WriteError,
        ReadError
    }

    /// <summary>
    /// Exception on the transport
    /// </summary>
    class TransportException : Exception
    {
        public TransportError Error { get; private set; }

        public TransportException(TransportError error)
            : base("Kafka transport error")
        {
            Error = error;
        }

        public TransportException(TransportError error, Exception exception)
            : base("Kafka transport error", exception)
        {
            Error = error;
        }
    }

    /// <summary>
    /// A SocketAsyncEventargs interface so that network is mockable.
    /// </summary>
    interface ISocketAsyncEventArgs
    {
        void SetBuffer(byte[] buffer, int offset, int count);
        void SetBuffer(int offset, int count);
        SocketError SocketError { get; }
        int BytesTransferred { get; }
        int Count { get; }
        int Offset { get; }
        byte[] Buffer { get; }
        object UserToken { get; set; }
        event Action<ISocket, ISocketAsyncEventArgs> Completed;
    }

    class RealSocketAsyncEventArgs : SocketAsyncEventArgs, ISocketAsyncEventArgs
    {
        public RealSocketAsyncEventArgs()
        {
            Completed += (o, saea) =>
            {
                var ev = _event;
                if (ev != null)
                {
                    ev(o as ISocket, this);
                }
            };
        }


        private Action<ISocket, ISocketAsyncEventArgs> _event;
        event Action<ISocket, ISocketAsyncEventArgs> ISocketAsyncEventArgs.Completed
        {
            add { _event += value; }
            remove { _event -= value; }
        }
    }

    /// <summary>
    /// A Socket interface so that network is mockable.
    /// </summary>
    interface ISocket
    {
        ISocketAsyncEventArgs CreateEventArgs();
        Task ConnectAsync();
        int Send(byte[] buffer, int offset, int size, SocketFlags flags, out SocketError error);
        bool SendAsync(ISocketAsyncEventArgs args);
        bool ReceiveAsync(ISocketAsyncEventArgs args);
        void Close();

        bool Connected { get; }
        bool Blocking { get; set; }
        int SendBufferSize { get; set; }
        int ReceiveBufferSize { get; set; }
    }

    class RealSocket : Socket, ISocket
    {
        private readonly EndPoint _endPoint;

        public RealSocket(EndPoint endPoint)
            : base(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        {
            _endPoint = endPoint;
        }

        public ISocketAsyncEventArgs CreateEventArgs()
        {
            return new RealSocketAsyncEventArgs();
        }

        public Task ConnectAsync()
        {
#if NET_CORE
            return this.ConnectAsync(_endPoint); // same as before, it uses FromAsync() helper
#else
            // Use old Begin/End API, this is much simpler than using Socket.Async
            // and we do not need "performance" here.
            return Task.Factory.FromAsync(BeginConnect, EndConnect, _endPoint, null);
#endif
        }

        public bool SendAsync(ISocketAsyncEventArgs args)
        {
            return SendAsync(args as SocketAsyncEventArgs);
        }

        public bool ReceiveAsync(ISocketAsyncEventArgs args)
        {
            return ReceiveAsync(args as SocketAsyncEventArgs);
        }

        void ISocket.Close()
        {
            Shutdown(SocketShutdown.Both);
            Dispose();
        }
    }

    /// <summary>
    /// Interface to a network connection
    /// </summary>
    interface IConnection : IDisposable
    {
        /// <summary>
        /// Send some data over the wire.
        /// </summary>
        /// <param name="correlationId">Correlation id associated to the request.</param>
        /// <param name="data">Data to send over the network.</param>
        /// <param name="acknowledge">An acknowledgement is expected for this request.</param>
        /// <returns>A future signaling when the send operation has terminated.</returns>
        Task SendAsync(int correlationId, ReusableMemoryStream data, bool acknowledge);

        /// <summary>
        /// Connect.
        /// </summary>
        /// <returns></returns>
        Task ConnectAsync();

        /// <summary>
        /// Emit a response. The given memory stream is suitable to be released to ReusableMemoryStream.
        /// </summary>
        event Action<IConnection, int, ReusableMemoryStream> Response;

        /// <summary>
        /// Emit an error.
        /// </summary>
        event Action<IConnection, Exception> ReceiveError;
    }

    /// <summary>
    /// This class is responsible for sending and receiving data over/from the network.
    /// It does not check for correctness of responses. The only knowledge of the protocol
    /// it uses is decoding the message size and correlation id from the responses.
    ///
    /// Send and Receive are fully pipelined. Receive are handled using the asynchronous
    /// event based Socket API (ReceiveAsync(SocketAsyncEventArgs)). For send we use the
    /// synchronous API with the socket in non blocking mode. When Send returns E_WOULDBLOCK,
    /// we fall back to the event based asynchronous API (SendAsync).
    /// </summary>
    class Connection : IConnection
    {
        private const int DefaultBufferSize = 8092;
        private const int SizeLength = sizeof (Int32);
        private const int CorrelationIdLength = sizeof (Int32);
        private const int HeaderLength = SizeLength + CorrelationIdLength;

        private readonly ISocket _socket;
        private readonly ISocketAsyncEventArgs _sendArgs;
        private readonly ISocketAsyncEventArgs _receiveArgs;

        // Pools
        private readonly Pool<byte[]> _bufferPool;
        private readonly Pool<ReusableMemoryStream> _responsePool;

        // The Kafka server ensures that acks are ordered on a given connection, we take
        // advantage of that by using a queue to store correlation ids.
        private readonly ConcurrentQueue<int> _correlationIds = new ConcurrentQueue<int>();

        private struct Void { } // Private type used with TaskCompletionSource<>
        private static readonly Void SuccessResult = new Void();
        private static readonly Task<Void> SuccessTask = Task.FromResult(SuccessResult);

        // Default ISocket implementation
        public static Func<EndPoint, ISocket> DefaultSocketFactory = ep => new RealSocket(ep);

        /// <summary>
        /// Build a connection object.
        /// </summary>
        /// <param name="host">Remote hoit to connect to</param>
        /// <param name="port">Remote port to connect to</param>
        /// <param name="socketFactory">The creation factory</param>
        /// <param name="bufferPool">A pool of buffers to use internally in socket APIs</param>
        /// <param name="responsePool">A pool of buffers to use for receive responses sent to the outside world</param>
        /// <param name="sendBufferSize">Size of the buffer used for send by the underlying socket</param>
        /// <param name="receiveBufferSize">Size of the buffer used for receive by the underlying socket</param>
        public Connection(string host, int port, Func<EndPoint, ISocket> socketFactory,
            Pool<byte[]> bufferPool,
            Pool<ReusableMemoryStream> responsePool,
            int sendBufferSize = DefaultBufferSize,
            int receiveBufferSize = DefaultBufferSize)
            : this(
#if NET_CORE
                new IPEndPoint(Dns.GetHostAddressesAsync(host).Result[0], port),
#else
                new IPEndPoint(Dns.GetHostAddresses(host)[0], port),
#endif
                socketFactory, bufferPool, responsePool, sendBufferSize, receiveBufferSize)
        {
        }

        public Connection(EndPoint endPoint, Func<EndPoint, ISocket> socketFactory,
            Pool<byte[]> bufferPool,
            Pool<ReusableMemoryStream> responsePool,
            int sendBufferSize,
            int receiveBufferSize)
        {
            _socket = socketFactory(endPoint);
            _socket.Blocking = false;
            _socket.SendBufferSize = sendBufferSize;
            _socket.ReceiveBufferSize = receiveBufferSize;
            _sendArgs = _socket.CreateEventArgs();
            _sendArgs.Completed += OnSendCompleted;
            _receiveArgs = _socket.CreateEventArgs();
            _receiveArgs.Completed += OnReceiveCompleted;
            _bufferPool = bufferPool;
            _responsePool = responsePool;
        }

        public async Task ConnectAsync()
        {
            try
            {
                await _socket.ConnectAsync();
            }
            catch (Exception ex)
            {
                throw new TransportException(TransportError.ConnectError, ex);
            }
            StartReceive();
        }

        #region Send

        public Task SendAsync(int correlationId, ReusableMemoryStream data, bool acknowledge)
        {
            if (!_socket.Connected)
            {
                throw new TransportException(TransportError.ConnectError);
            }

            if (acknowledge)
            {
                _correlationIds.Enqueue(correlationId);
            }

            // Sending will use synchronous send first using non blocking mode on the socket.
            // If we cannot send all bytes in one call, we switch to an asynchronous send loop.
            var future = SuccessTask;
            var buffer = _bufferPool.Reserve();
            try
            {
                data.Position = 0;
                int read = data.Read(buffer, 0, buffer.Length);
                while (read != 0)
                {
                    SocketError error;
                    int sent = _socket.Send(buffer, 0, read, SocketFlags.None, out error);
                    if (error == SocketError.WouldBlock || (error == SocketError.Success && sent < read))
                    {
                        // Start an async send loop
                        _sendContext.Data = data;
                        _sendContext.Promise = new TaskCompletionSource<Void>();
                        _sendContext.Buffer = buffer;
                        _sendArgs.UserToken = this;
                        _sendArgs.SetBuffer(buffer, sent, read - sent);
                        future = _sendContext.Promise.Task;
                        if (!_socket.SendAsync(_sendArgs))
                        {
                            OnSendCompleted(_socket, _sendArgs);
                        }
                        break;
                    }
                    else if (error != SocketError.Success)
                    {
                        throw new SocketException((int) error);
                    }
                    read = data.Read(buffer, 0, buffer.Length);
                }
                if (read == 0)
                {
                    _bufferPool.Release(buffer);
                }
            }
            catch (Exception ex)
            {
                CleanSend();
                throw new TransportException(TransportError.WriteError, ex);
            }

            return future;
        }

        class SendContext
        {
            public ReusableMemoryStream Data;
            public TaskCompletionSource<Void> Promise;
            public byte[] Buffer;
        }

        private readonly SendContext _sendContext = new SendContext();

        private int _recursiveOnSendCompleted; // count recursive calls when Socket.SendAsync returns synchronously

        private void CleanSend()
        {
            CleanAsyncArgs(_sendArgs);
            _bufferPool.Release(_sendContext.Buffer);
            _sendContext.Buffer = null;
            _sendContext.Data = null;
        }

        // Async send loop body
        private static void OnSendCompleted(ISocket sender, ISocketAsyncEventArgs saea)
        {
            var connection = saea.UserToken as Connection;
            if (connection == null)
            {
                // It should not happen but it makes Sonar happy
                CleanAsyncArgs(saea);
                return;
            }

            if (saea.SocketError != SocketError.Success)
            {
                connection.CleanSend();
                connection._sendContext.Promise.SetException(new TransportException(TransportError.WriteError,
                    new SocketException((int) saea.SocketError)));
                return;
            }

            // Async loop
            if (saea.BytesTransferred != saea.Count)
            {
                connection.LoopSend(saea.Offset + saea.BytesTransferred, saea.Count - saea.BytesTransferred);
            }
            else
            {
                int read = connection._sendContext.Data.Read(connection._sendContext.Buffer, 0,
                    connection._sendContext.Buffer.Length);
                if (read != 0)
                {
                    connection.LoopSend(0, read);
                }
                else
                {
                    connection.CleanSend();
                    connection._sendContext.Promise.SetResult(SuccessResult);
                }
            }
        }

        private void LoopSend(int from, int count)
        {
            var saea = _sendArgs;
            try
            {
                saea.SetBuffer(from, count);
                if (!_socket.SendAsync(saea))
                {
                    if (++_recursiveOnSendCompleted > 20)
                    {
                        // Too many recursive calls, we trampoline out of the current
                        // stack trace using a simple Task. This should really not happen
                        // but you never know.
                        _recursiveOnSendCompleted = 0;
                        Task.Run(() => OnSendCompleted(_socket, saea));
                        return;
                    }
                    OnSendCompleted(_socket, saea);
                }
            }
            catch (Exception ex)
            {
                CleanSend();
                _sendContext.Promise.SetException(new TransportException(TransportError.WriteError, ex));
            }
        }

        #endregion

        #region Receive

        // Receive steps
        enum ReceiveState
        {
            None,
            Header,
            Body
        }

        class ReceiveContext
        {
            public ReceiveState State = ReceiveState.Header;
            public int CorrelationId;
            public int RemainingExpected;
            public byte[] Buffer;
            public ReusableMemoryStream Response;
        }

        private readonly ReceiveContext _receiveContext = new ReceiveContext();

        private void CleanReceive(bool cleanResponse)
        {
            CleanAsyncArgs(_receiveArgs);
            _bufferPool.Release(_receiveContext.Buffer);
            _receiveContext.Buffer = null;
            _receiveContext.CorrelationId = 0;
            _receiveContext.RemainingExpected = 0;
            _receiveContext.State = ReceiveState.None;
            if (cleanResponse && _receiveContext.Response != null)
            {
                _receiveContext.Response.Dispose();
            }
            _receiveContext.Response = null;
        }

        // Start a receive sequence (read header, then body)
        private void StartReceive()
        {
            try
            {
                // First we expect a header which is always size(4 bytes) + correlation(4 bytes)
                _receiveContext.State = ReceiveState.Header;
                _receiveContext.CorrelationId = 0;
                _receiveContext.RemainingExpected = HeaderLength;
                _receiveContext.Buffer = _bufferPool.Reserve();
                _receiveContext.Response = null;
                _receiveArgs.SetBuffer(_receiveContext.Buffer, 0, _receiveContext.RemainingExpected);
                _receiveArgs.UserToken = this;

                // Receive async loop
                if (!_socket.ReceiveAsync(_receiveArgs))
                {
                    OnReceiveCompleted(_socket, _receiveArgs);
                }
            }
            catch (Exception ex)
            {
                CleanReceive(true);
                OnReceiveError(new TransportException(TransportError.ReadError, ex));
            }
        }

        private int _recursiveOnReceiveCompleted; // count recursive calls when Socket.ReceiveAsync returns synchronously

        // Async receive loop
        private static void OnReceiveCompleted(ISocket sender, ISocketAsyncEventArgs saea)
        {
            var connection = saea.UserToken as Connection;
            if (connection == null) // This one should not  happen but it makes Sonar happy
            {
                CleanAsyncArgs(saea);
                return;
            }

            if (saea.SocketError == SocketError.OperationAborted)
            {
                connection.CleanReceive(true);
                return;
            }

            if (saea.SocketError != SocketError.Success || saea.BytesTransferred == 0)
            {
                connection.CleanReceive(true);
                connection.OnReceiveError(new TransportException(TransportError.ReadError,
                    new SocketException(saea.SocketError != SocketError.Success
                        ? (int) saea.SocketError
                        : (int) SocketError.ConnectionAborted)));
                return;
            }

            try
            {
                // Loop if needed
                if (saea.BytesTransferred != saea.Count)
                {
                    saea.SetBuffer(saea.Offset + saea.BytesTransferred, saea.Count - saea.BytesTransferred);
                    if (sender.ReceiveAsync(saea)) return;
                    if (++connection._recursiveOnReceiveCompleted > 20)
                    {
                        // Too many recursive calls, we trampoline out of the current
                        // stack trace using a simple Task. This should really not happen
                        // but you never know.
                        connection._recursiveOnReceiveCompleted = 0;
                        Task.Run(() => OnReceiveCompleted(sender, saea));
                        return;
                    }
                    OnReceiveCompleted(sender, saea);
                    return;
                }

                // Handle current state
                switch (connection._receiveContext.State)
                {
                    case ReceiveState.Header:
                        connection.HandleHeaderState(connection._receiveContext, sender, saea);
                        break;

                    case ReceiveState.Body:
                        connection.HandleBodyState(connection._receiveContext, sender, saea);
                        break;

                    default:
                        throw new InvalidOperationException(
                            string.Format("Receive state should be Header or Body only (was: {0}).", connection._receiveContext.State));
                }
            }
            catch (CorrelationException ex)
            {
                connection.CleanReceive(true);
                connection.OnReceiveError(ex);
            }
            catch (Exception ex)
            {
                connection.CleanReceive(true);
                connection.OnReceiveError(new TransportException(TransportError.ReadError, ex));
            }
        }

        // Extract size and correlation Id, then start receive body loop.
        private void HandleHeaderState(ReceiveContext context, ISocket socket, ISocketAsyncEventArgs saea)
        {
            int responseSize = BigEndianConverter.ToInt32(saea.Buffer);
            int correlationId = BigEndianConverter.ToInt32(saea.Buffer, SizeLength);
            // TODO check absurd response size?

            int matching;
            if (!_correlationIds.TryDequeue(out matching) || matching != correlationId)
            {
                throw new CorrelationException(matching, correlationId);
            }

            context.State = ReceiveState.Body;
            context.CorrelationId = correlationId;
            // responseSize includes 4 bytes of correlation id
            context.RemainingExpected = responseSize - CorrelationIdLength;
            context.Response = _responsePool.Reserve();
            saea.SetBuffer(0, Math.Min(context.Buffer.Length, context.RemainingExpected));
            if (!socket.ReceiveAsync(saea))
            {
                OnReceiveCompleted(socket, saea);
            }
        }

        // Just pass back the response
        private void HandleBodyState(ReceiveContext context, ISocket socket, ISocketAsyncEventArgs saea)
        {
            int rec = Math.Min(saea.Buffer.Length, context.RemainingExpected);
            context.Response.Write(saea.Buffer, 0, rec);
            context.RemainingExpected -= rec;

            if (context.RemainingExpected == 0)
            {
                context.Response.Position = 0;
                OnResponse(context.CorrelationId, context.Response);
                CleanReceive(false);
                StartReceive();
            }
            else
            {
                saea.SetBuffer(0, Math.Min(context.Buffer.Length, context.RemainingExpected));
                if (!socket.ReceiveAsync(saea))
                {
                    OnReceiveCompleted(socket, saea);
                }
            }
        }

        #endregion

        private static void CleanAsyncArgs(ISocketAsyncEventArgs args)
        {
            args.UserToken = null;
            args.SetBuffer(null, 0, 0);
        }

        public event Action<IConnection, int, ReusableMemoryStream> Response;
        public event Action<IConnection, Exception> ReceiveError;

        private void OnResponse(int c, ReusableMemoryStream b)
        {
            var ev = Response;
            if (ev != null)
                ev(this, c, b);
        }

        private void OnReceiveError(Exception e)
        {
            var ev = ReceiveError;
            if (ev != null)
                ev(this, e);
        }

        public void Dispose()
        {
            _socket.Close();
        }
    }
}
