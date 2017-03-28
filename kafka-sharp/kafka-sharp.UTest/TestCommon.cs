using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Text;
using Kafka.Cluster;
using Kafka.Common;
using Kafka.Public;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestCommon
    {
        [Test]
        public void TestReusableMemoryStream()
        {
            var pool = new Pools(new Statistics());
            pool.InitRequestsBuffersPool();
            using (var stream = pool.RequestsBuffersPool.Reserve())
            {
                Assert.AreEqual(0, stream.Length);
                Assert.AreEqual(0, stream.Position);
                Assert.IsTrue(stream.CanWrite);
                Assert.IsTrue(stream.CanRead);

                var b = Encoding.UTF8.GetBytes("I see dead beef people");
                stream.Write(b, 0, b.Length);
            }

            var s = pool.RequestsBuffersPool.Reserve();
            s.Dispose();
            Assert.AreSame(s, pool.RequestsBuffersPool.Reserve());
        }

        class Item
        {
            public int Value;
        }

        [Test]
        public void TestPool()
        {
            int over = 0;
            var pool = new Pool<Item>(6, () => new Item(), (i, b) =>
            {
                i.Value = 0;
                if (!b) ++over;
            });

            var item = pool.Reserve();
            Assert.IsNotNull(item);

            item.Value = 10;
            pool.Release(item);
            Assert.That(item.Value, Is.EqualTo(0));
            Assert.That(pool.Watermark, Is.EqualTo(1));

            var items = Enumerable.Range(0, 10).Select(i => pool.Reserve()).ToList();
            foreach (var i in items)
            {
                pool.Release(i);
            }
            Assert.That(pool.Watermark, Is.EqualTo(6));
            Assert.That(over, Is.EqualTo(4));

            Assert.That(() => pool.Release(null), Throws.Nothing);
            Assert.That(() => new Pool<Item>(() => new Item(), null), Throws.InstanceOf<ArgumentNullException>());
            Assert.That(() => new Pool<Item>(null, (i, _) => i.Value = 0), Throws.InstanceOf<ArgumentNullException>());
        }

        [Test]
        public void TestTimestamp()
        {
            Assert.AreEqual(new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), Timestamp.Epoch);

            var now = DateTime.UtcNow;
            var datenow = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Millisecond, DateTimeKind.Utc);

            Assert.AreEqual(datenow, Timestamp.FromUnixTimestamp(Timestamp.ToUnixTimestamp(now)));
        }
    }
}