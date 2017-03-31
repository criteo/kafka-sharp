using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Common
{
    static class Timestamp
    {
        public static DateTime Epoch
        {
            get { return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc); }
        }

        public static long Now { get { return ToUnixTimestamp(DateTime.UtcNow); } }

        public static long ToUnixTimestamp(DateTime datetime)
        {
            return (long)datetime.ToUniversalTime().Subtract(Epoch).TotalMilliseconds;
        }

        public static DateTime FromUnixTimestamp(long timestamp)
        {
            return Epoch.Add(TimeSpan.FromMilliseconds(timestamp));
        }
    }
}
