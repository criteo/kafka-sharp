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
    }
}