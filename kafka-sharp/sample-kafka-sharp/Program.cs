using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Public;

namespace sample_kafka_sharp
{
    class Program
    {
        private class ConsoleLogger : ILogger
        {
            private static void LogHeader(string header)
            {
                Console.Write("[{0:yyyy-MM-dd HH:mm:ss}] {1} ", DateTime.UtcNow, header);
            }

            public void LogInformation(string message)
            {
                LogHeader("INFO");
                Console.WriteLine(message);
            }

            public void LogWarning(string message)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                LogHeader("WARNING");
                Console.ResetColor();
                Console.WriteLine(message);
            }

            public void LogError(string message)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                LogHeader("ERROR");
                Console.ResetColor();
                Console.WriteLine(message);
            }
        }

        private static volatile bool _running = true;
        private static Random _random = new Random();
        private static string[] _values =
        {
            "loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong",
            "The quick brown fox jumps over the lazy dog.",
            "Some random data",
            @"fhsdjatrgfvbnuhuvhi\\jfdgh dahgilsdah wlFIWE TGFHJERFLHS\\KDB OGFDLSKNLFD,GJLFSKDGHJFDLKHJLKFDHKK\\HFLKH KHFLHSDGLFSKDGHLFDKGjskghjlfkdgnfdlgndkkdlghb",
            "bulbe",
            "ujdifhgob m",
            @"hlfkdhshgf
dshfhjfsgj
dfhfdhjgfjgfsj
adghfhfdhfdhda
dagfhefdghafdahfh",
                  "carcajou",
                  "e6692333-8e76-4b7b-b3f6-5fd5c20cb741"
        };

        private static string[] _topics =
        {
            "rep-1-p-3",
            "rep-2-p-1",
            "rep-2-p-2",
            "rep-2-p-3",
            "test"
        };

        enum Mode
        {
            Stress,
            StressHard,
            Profile
        }

        private static void Main(string[] args)
        {
            var cluster =
                new Cluster(
                    new Configuration
                    {
                        BatchSize = 5000,
                        BufferingTime = TimeSpan.FromMilliseconds(200),
                        MessageTtl = TimeSpan.FromSeconds(30),
                        ErrorStrategy = ErrorStrategy.Discard,
                        //Seeds = "192.168.0.215:9092,192.168.0.215:9093,192.168.0.215:9094",
                        Seeds = "192.168.18.48:9092,192.168.18.48:9093,192.168.18.48:9094",
                        MaxBufferedMessages = -1, //30000,
                        CompressionCodec = CompressionCodec.Snappy,
                        RequiredAcks = -1
                    }, new ConsoleLogger());
            var mode = Mode.Stress;
            var task = Start(mode, cluster);
            Console.ReadKey();
            _running = false;
            Console.ReadKey();
            task.Wait();
            Console.WriteLine(cluster.Statistics);
            Console.ReadKey();
            cluster.Dispose();
        }

        static Task Start(Mode mode, Cluster cluster)
        {
            var list = new List<Task>();
            for (int i = 0; i < Environment.ProcessorCount; ++i)
            {
                list.Add(Loop(mode, cluster, i));
            }

            return Task.WhenAll(list);
        }

        static async Task Loop(Mode mode, Cluster cluster, int id)
        {
            Console.WriteLine("Starting worker " + id);
            await Task.Yield();
            var random = new Random();
            int i = 0;
            while (_running)
            {
                var topic = _topics[random.Next(_topics.Length)];
                var data = " " + _values[random.Next(_values.Length)];
                cluster.Produce(topic, string.Format("{0} {1} {2} {3}", topic, id, ++i, data));

                switch (mode)
                {
                    case Mode.Profile:
                        if (i%3 == 0)
                            await Task.Delay(15);
                        break;

                    case Mode.Stress:
                        if (i%100 == 0)
                            await Task.Delay(15);
                        break;

                    case Mode.StressHard:
                        if (i%500 == 0)
                            await Task.Delay(1);
                        break;
                }
            }
        }
    }
}
