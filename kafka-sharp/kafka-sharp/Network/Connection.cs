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

    enum TransportError
    {
        ConnectError,
        WriteError,
        ReadError
    }

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

    interface IConnection : IDisposable
    {
        Task SendAsync(int correlationId, byte[] buffer, bool acknowledge);
        Task ConnectAsync();
        event Action<IConnection, int, byte[]> Response;
        event Action<IConnection, Exception> ReceiveError;
    }

    /// <summary>
    /// This class is responsible foir sending and receiving data over/from the network.
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

        private readonly Socket _socket;
        private readonly SocketAsyncEventArgs _sendArgs;
        private readonly SocketAsyncEventArgs _receiveArgs;
        private readonly EndPoint _endPoint;
        private readonly byte[] _headerBuffer = new byte[8];

        // The Kafka server ensures that acks are ordered on a given connection, we take
        // advantage of that by using a queue to store correlation ids.
        private readonly ConcurrentQueue<int> _correlationIds = new ConcurrentQueue<int>();

        private struct Void { }
        private static readonly Void SuccessResult = new Void();
        private static readonly Task<Void> SuccessTask = Task.FromResult(SuccessResult);

        public Connection(string host, int port, int sendBufferSize = DefaultBufferSize,
                          int receiveBufferSize = DefaultBufferSize)
            : this(new IPEndPoint(Dns.GetHostEntry(host).AddressList[0], port), sendBufferSize, receiveBufferSize)
        {
        }

        public Connection(EndPoint endPoint, int sendBufferSize = DefaultBufferSize,
                          int receiveBufferSize = DefaultBufferSize)
        {
            _socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                {
                    Blocking = false,
                    SendBufferSize = sendBufferSize,
                    ReceiveBufferSize = receiveBufferSize,
                };
            _sendArgs = new SocketAsyncEventArgs();
            _sendArgs.Completed += OnSendCompleted;
            _receiveArgs = new SocketAsyncEventArgs();
            _receiveArgs.Completed += OnReceiveCompleted;
            _endPoint = endPoint;
        }

        public Task SendAsync(int correlationId, byte[] buffer, bool acknowledge)
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
            // If we cannot send all byte in one call, we switch to an asynchronous send loop.
            var future = SuccessTask;
            try
            {
                SocketError error;
                int sent = _socket.Send(buffer, 0, buffer.Length, SocketFlags.None, out error);
                if (error == SocketError.WouldBlock || sent < buffer.Length)
                {
                    var promise = new TaskCompletionSource<Void>();
                    _sendArgs.UserToken = promise;
                    _sendArgs.SetBuffer(buffer, sent, buffer.Length - sent);
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

        private int _recursiveOnSendCompleted;

        private void OnSendCompleted(object sender, SocketAsyncEventArgs saea)
        {
            var promise = saea.UserToken as TaskCompletionSource<Void>;
            if (saea.SocketError != SocketError.Success)
            {
                promise.SetException(new TransportException(TransportError.WriteError,
                                                            new SocketException((int) saea.SocketError)));
                return;
            }

            if (saea.BytesTransferred != saea.Count)
            {
                try
                {
                    saea.SetBuffer(saea.Offset + saea.BytesTransferred, saea.Count - saea.BytesTransferred);
                    if (!_socket.SendAsync(saea))
                    {
                        if (++_recursiveOnSendCompleted > 20)
                        {
                            // Trampoline out of stack
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
                await Task.Factory.FromAsync(_socket.BeginConnect, _socket.EndConnect, _endPoint, null);
            }
            catch (Exception ex)
            {
                throw new TransportException(TransportError.ConnectError, ex);
            }
            StartReceive();
        }

        enum ReceiveState
        {
            Header,
            Body
        }

        class ReceiveContext
        {
            public ReceiveState State = ReceiveState.Header;
            public int CorrelationId;
        }

        private readonly ReceiveContext _receiveContext = new ReceiveContext();

        private void StartReceive()
        {
            try
            {
                // First we expect a header which is always size(4 bytes) + correlation(4 bytes)
                _receiveContext.State = ReceiveState.Header;
                _receiveContext.CorrelationId = 0;
                _receiveArgs.SetBuffer(_headerBuffer, 0, 8);
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

        private int _recursiveOnReceiveCompleted = 0;
        private void OnReceiveCompleted(object sender, SocketAsyncEventArgs saea)
        {
            if (saea.SocketError != SocketError.Success || saea.BytesTransferred == 0)
            {
                OnReceiveError(new TransportException(TransportError.ReadError,
                                                      new SocketException(saea.SocketError != SocketError.Success
                                                                              ? (int) saea.SocketError
                                                                              : (int)SocketError.ConnectionAborted)));
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
                            // Trampoline out of stack
                            _recursiveOnReceiveCompleted = 0;
                            Task.Factory.StartNew(() => OnReceiveCompleted(sender, saea));
                            return;
                        }
                        OnReceiveCompleted(sender, saea);
                    }
                    return;
                }

                // Handle current state
                var context = saea.UserToken as ReceiveContext;
                switch (context.State)
                {
                    case ReceiveState.Header:
                        HandleHeaderState(context, sender as Socket, saea);
                        break;

                    case ReceiveState.Body:
                        HandleBodyState(context, saea);
                        break;
                }
            }
            catch (CorrelationException ex)
            {
                OnReceiveError(ex);
            }
            catch (Exception ex)
            {
                OnReceiveError(new TransportException(TransportError.ReadError, ex));
            }
        }

        // Extract size and correlation Id, then start receive body loop.
        private void HandleHeaderState(ReceiveContext context, Socket socket, SocketAsyncEventArgs saea)
        {
            int responseSize = BigEndianConverter.ToInt32(saea.Buffer);
            int correlationId = BigEndianConverter.ToInt32(saea.Buffer, 4);
            // TODO check absurd response size?

            int matching;
            if (!_correlationIds.TryDequeue(out matching) || matching != correlationId)
            {
                throw new CorrelationException(matching, correlationId);
            }

            context.State = ReceiveState.Body;
            context.CorrelationId = correlationId;
            // responseSize includes 4 bytes of correlation id
            saea.SetBuffer(new byte[responseSize - 4], 0, responseSize - 4);
            if (!socket.ReceiveAsync(saea))
            {
                OnReceiveCompleted(socket, saea);
            }
        }

        // Just pass back the response
        private void HandleBodyState(ReceiveContext context, SocketAsyncEventArgs saea)
        {
            OnResponse(context.CorrelationId, saea.Buffer);
            StartReceive();
        }

        public event Action<IConnection, int, byte[]> Response;
        public event Action<IConnection, Exception> ReceiveError;

        private void OnResponse(int c, byte[] b)
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
            _socket.Dispose();
        }
    }
}
