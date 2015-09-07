using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Common;
using NUnit.Framework;

namespace tests_kafka_sharp
{
    [TestFixture]
    class TestCommon
    {
        [Test]
        public void TestReusableMemoryStream()
        {
            using (var stream = ReusableMemoryStream.Reserve())
            {
                Assert.AreEqual(0, stream.Length);
                Assert.AreEqual(0, stream.Position);
                Assert.IsTrue(stream.CanWrite);
                Assert.IsTrue(stream.CanRead);

                var b = Encoding.UTF8.GetBytes("I see dead beef people");
                stream.Write(b, 0, b.Length);
            }

            using (var stream = ReusableMemoryStream.Reserve(1024))
            {
                Assert.AreEqual(1024, stream.Length);
                Assert.AreEqual(0, stream.Position);
                Assert.GreaterOrEqual(1024, stream.Capacity);
            }

            var s = ReusableMemoryStream.Reserve();
            s.Dispose();
            Assert.AreSame(s, ReusableMemoryStream.Reserve()); //  Won't work if tests are in parallel
        }

        class Item
        {
            public int Value;
        }

        [Test]
        public void TestPool()
        {
            var pool = new Pool<Item>(5, () => new Item(), i => i.Value = 0);

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
            Assert.That(pool.Watermark, Is.EqualTo(5));

            Assert.That(() => pool.Release(null), Throws.Nothing);
            Assert.That(() => new Pool<Item>(() => new Item(), null), Throws.InstanceOf<ArgumentNullException>());
            Assert.That(() => new Pool<Item>(null, i => i.Value = 0), Throws.InstanceOf<ArgumentNullException>());
        }
    }
}