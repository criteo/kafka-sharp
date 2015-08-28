// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

using Kafka.Common;

namespace Kafka.Protocol
{
    // Convenience interface to avoid code duplication.
    // quite ugly from OO perspective but struct cannot inherit in C#.
    interface ISerializableRequest
    {
        ReusableMemoryStream Serialize(int correlationId, byte[] clientId);
        void SerializeBody(ReusableMemoryStream stream);
    }

    // Convenience class to avoid code duplication. We cannot
    // use inheritance with structs so we resort to this dumb trick
    // and ugly ISerializableRequest.
    static class CommonRequest
    {
        public static ReusableMemoryStream Serialize<TRequest>(TRequest request, int correlationId, byte[] clientId,
            Basics.ApiKey apiKey) where TRequest : ISerializableRequest
        {
            var stream = ReusableMemoryStream.Reserve();
            Basics.WriteRequestHeader(stream, correlationId, apiKey, clientId);
            request.SerializeBody(stream);
            stream = Basics.WriteMessageLength(stream);
            stream.Position = 0;
            return stream;
        }
    }
}