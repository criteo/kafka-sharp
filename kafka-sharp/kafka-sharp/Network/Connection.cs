// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Kafka.Common;

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

        // Use old Begin/End API, this is much simpler than using Socket.Async
        // and we do not need "performance" here.
        public Task ConnectAsync()
        {
            return Task.Factory.FromAsync(BeginConnect, EndConnect, _endPoint, null);
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
        private readonly byte[] _receiveBuffer;

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
        /// <param name="sendBufferSize">Size of the buffer used for send by the underlying socket</param>
        /// <param name="receiveBufferSize">Size of the buffer used for receive by the underlying socket</param>
        public Connection(string host, int port, Func<EndPoint, ISocket> socketFactory,
            int sendBufferSize = DefaultBufferSize,
            int receiveBufferSize = DefaultBufferSize)
            : this(
                new IPEndPoint(Dns.GetHostAddresses(host)[0], port), socketFactory, sendBufferSize,
                receiveBufferSize)
        {
        }

        public Connection(EndPoint endPoint, Func<EndPoint, ISocket> socketFactory,
            int sendBufferSize = DefaultBufferSize,
            int receiveBufferSize = DefaultBufferSize)
        {
            _socket = socketFactory(endPoint);
            _socket.Blocking = false;
            _socket.SendBufferSize = sendBufferSize;
            _socket.ReceiveBufferSize = receiveBufferSize;
            _sendArgs = _socket.CreateEventArgs();
            _sendArgs.Completed += OnSendCompleted;
            _receiveArgs = _socket.CreateEventArgs();
            _receiveArgs.Completed += OnReceiveCompleted;
            _receiveBuffer = new byte[Math.Max(HeaderLength, receiveBufferSize)];
        }

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
            try
            {
                SocketError error;
                int sent = _socket.Send(data.GetBuffer(), 0, (int) data.Length, SocketFlags.None, out error);
                if (error == SocketError.WouldBlock || sent < data.Length)
                {
                    // Start an async send loop
                    var promise = new TaskCompletionSource<Void>();
                    _sendArgs.UserToken = promise;
                    _sendArgs.SetBuffer(data.GetBuffer(), sent, (int) data.Length - sent);
                    if (!_socket.SendAsync(_sendArgs))
                    {
                        OnSendCompleted(_socket, _sendArgs);
                    }
                    future = promise.Task;
                }
            }
            catch (Exception ex)
            {
                throw new TransportException(TransportError.WriteError, ex);
            }

            return future;
        }

        private int _recursiveOnSendCompleted; // count recursive calls when Socket.SendAsync returns synchronously

        // Async send loop body
        private void OnSendCompleted(ISocket sender, ISocketAsyncEventArgs saea)
        {
            var promise = saea.UserToken as TaskCompletionSource<Void>;
            if (saea.SocketError != SocketError.Success)
            {
                promise.SetException(new TransportException(TransportError.WriteError,
                                                            new SocketException((int) saea.SocketError)));
                return;
            }

            // Async loop
            if (saea.BytesTransferred != saea.Count)
            {
                try
                {
                    saea.SetBuffer(saea.Offset + saea.BytesTransferred, saea.Count - saea.BytesTransferred);
                    if (!_socket.SendAsync(saea))
                    {
                        if (++_recursiveOnSendCompleted > 20)
                        {
                            // Too many recursive calls, we trampoline out of the current
                            // stack trace using a simple Task. This should really not happen
                            // but you never know.
                            _recursiveOnSendCompleted = 0;
                            Task.Factory.StartNew(() => OnSendCompleted(sender, saea));
                            return;
                        }
                        OnSendCompleted(sender, saea);
                    }
                    return;
                }
                catch (Exception ex)
                {
                    throw new TransportException(TransportError.WriteError, ex);
                }
            }

            promise.SetResult(SuccessResult);
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

        // Receive steps
        enum ReceiveState
        {
            Header,
            Body
        }

        class ReceiveContext
        {
            public ReceiveState State = ReceiveState.Header;
            public int CorrelationId;
            public int RemainingExpected;
            public ReusableMemoryStream Response;
        }

        private readonly ReceiveContext _receiveContext = new ReceiveContext();

        // Start a receive sequence (read header, then body)
        private void StartReceive()
        {
            try
            {
                // First we expect a header which is always size(4 bytes) + correlation(4 bytes)
                _receiveContext.State = ReceiveState.Header;
                _receiveContext.CorrelationId = 0;
                _receiveContext.RemainingExpected = HeaderLength;
                _receiveContext.Response = null;
                _receiveArgs.SetBuffer(_receiveBuffer, 0, _receiveContext.RemainingExpected);
                _receiveArgs.UserToken = _receiveContext;

                // Receive async loop
                if (!_socket.ReceiveAsync(_receiveArgs))
                {
                    OnReceiveCompleted(_socket, _receiveArgs);
                }
            }
            catch (Exception ex)
            {
                OnReceiveError(new TransportException(TransportError.ReadError, ex));
            }
        }

        private int _recursiveOnReceiveCompleted; // count recursive calls when Socket.ReceiveAsync returns synchronously

        // Async receive loop
        private void OnReceiveCompleted(ISocket sender, ISocketAsyncEventArgs saea)
        {
            var context = saea.UserToken as ReceiveContext;
            if (saea.SocketError != SocketError.Success || saea.BytesTransferred == 0)
            {
                OnReceiveError(new TransportException(TransportError.ReadError,
                    new SocketException(saea.SocketError != SocketError.Success
                        ? (int) saea.SocketError
                        : (int) SocketError.ConnectionAborted)));
                if (context.Response != null)
                {
                    context.Response.Dispose();
                }
                return;
            }

            try
            {
                // Loop if needed
                if (saea.BytesTransferred != saea.Count)
                {
                    saea.SetBuffer(saea.Offset + saea.BytesTransferred, saea.Count - saea.BytesTransferred);
                    if (!_socket.ReceiveAsync(saea))
                    {
                        if (++_recursiveOnReceiveCompleted > 20)
                        {
                            // Too many recursive calls, we trampoline out of the current
                            // stack trace using a simple Task. This should really not happen
                            // but you never know.
                            _recursiveOnReceiveCompleted = 0;
                            Task.Factory.StartNew(() => OnReceiveCompleted(sender, saea));
                            return;
                        }
                        OnReceiveCompleted(sender, saea);
                    }
                    return;
                }

                // Handle current state
                switch (context.State)
                {
                    case ReceiveState.Header:
                        HandleHeaderState(context, sender, saea);
                        break;

                    case ReceiveState.Body:
                        HandleBodyState(context, sender, saea);
                        break;
                }
            }
            catch (CorrelationException ex)
            {
                OnReceiveError(ex);
            }
            catch (Exception ex)
            {
                if (context.Response != null)
                {
                    context.Response.Dispose();
                }
                OnReceiveError(new TransportException(TransportError.ReadError, ex));
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
            context.Response = ReusableMemoryStream.Reserve(context.RemainingExpected);
            saea.SetBuffer(0, Math.Min(_receiveBuffer.Length, context.RemainingExpected));
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
                StartReceive();
            }
            else
            {
                saea.SetBuffer(0, Math.Min(_receiveBuffer.Length, context.RemainingExpected));
                if (!socket.ReceiveAsync(saea))
                {
                    OnReceiveCompleted(socket, saea);
                }
            }
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
            _sendArgs.Completed -= OnSendCompleted;
            _receiveArgs.Completed -= OnReceiveCompleted;
        }
    }
}
