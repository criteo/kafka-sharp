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

        private static string[] _values =
            {
                "loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong"
                ,
                "The quick brown fox jumps over the lazy dog.",
                "Some random data",
                @"fhsdjatrgfvbnuhuvhi\\jfdgh dahgilsdah wlFIWE TGFHJERFLHS\\KDB OGFDLSKNLFD,GJLFSKDGHJFDLKHJLKFDHKK\\HFLKH KHFLHSDGLFSKDGHLFDKGjskghjlfkdgnfdlgndkkdlghb"
                ,
                "bulbe",
                "ujdifhgob m",
                @"hlfkdhshgf
dshfhjfsgj
dfhfdhjgfjgfsj
adghfhfdhfdhda
dagfhefdghafdahfh",
                "carcajou",
                "e6692333-8e76-4b7b-b3f6-5fd5c20cb741",
                "This is Spartaaaaaaaaaaaaaaaaaaaaaaaaaaaa!",
                "42",
                "sdafaeawafe3w"
            };

        private static string[] _topics;

        enum Mode
        {
            Stress,
            StressHard,
            Profile
        }

        private static void Usage()
        {
            const string options = @"
   --seeds ""host1:port,host2:port2,...,hostn:portn"" => broker nodes to bootstrap the cluster - MANDATORY
   --topics ""topic1,...,topicn"" => topics to dispatch messages on - MANDATORY
   --stress              => moderate amount of stress on  the system
   --hard                => hard stress on the system
   --discard             => discard messages in case of transport errors
   --retry               => retry send in case of transport errors
   --gzip                => use gzip compression
   --snappy              => use snappy compression
   --no-ack              => tell broker to not send acks (default is ack on sync leader only)
   --all-sync-ack        => tell broker to send ack when all live replicas have been synced
   --ttl TTL             => message time to live before been expired when retrying
   --batch SIZE          => size of message batches
   --time TIME           => max time to wait for batches to fill (in ms)
   --max-messages MAX    => max number of messages in the system before blocking producers
   --clientid ID         => client id of the producer
   --concurrency PAR     =>  max concurrency used by the system
   --timeout TIME        => Kafka request timeout (broker side)
   --send-buffer SIZE    => socket system buffer size for send
   --receive-buffer SIZE => socket system buffer size for receive";
            Console.WriteLine("Options are:");
            Console.WriteLine(options);
        }

        private static void Main(string[] args)
        {
            Mode mode = Mode.Profile;
            var configuration = new Configuration();

            // Ugly command line parsing
            string curOpt = "";
            try
            {
                bool seeds = false;
                bool topics = false;
                for (int i = 0; i < args.Length; ++i)
                {
                    curOpt = args[i];
                    switch (args[i])
                    {
                        case "--stress":
                            mode = Mode.Stress;
                            break;

                        case "--hard":
                            mode = Mode.StressHard;
                            break;

                        case "--discard":
                            configuration.ErrorStrategy = ErrorStrategy.Discard;
                            break;

                        case "--retry":
                            configuration.ErrorStrategy = ErrorStrategy.Retry;
                            break;

                        case "--gzip":
                            configuration.CompressionCodec = CompressionCodec.Gzip;
                            break;

                        case "--snappy":
                            configuration.CompressionCodec = CompressionCodec.Snappy;
                            break;

                        case "--no-ack":
                            configuration.RequiredAcks = RequiredAcks.None;
                            break;

                        case "--all-sync-ack":
                            configuration.RequiredAcks = RequiredAcks.AllInSyncReplicas;
                            break;

                        case "--ttl":
                            configuration.MessageTtl = TimeSpan.FromSeconds(int.Parse(args[++i]));
                            break;

                        case "--batch":
                            configuration.BatchSize = int.Parse(args[++i]);
                            break;

                        case "--time":
                            configuration.BufferingTime = TimeSpan.FromMilliseconds(int.Parse(args[++i]));
                            break;

                        case "--max-messages":
                            configuration.MaxBufferedMessages = int.Parse(args[++i]);
                            break;

                        case "--topics":
                            topics = true;
                            _topics = args[++i].Split(',');
                            break;

                        case "--seeds":
                            seeds = true;
                            configuration.Seeds = args[++i];
                            break;

                        case "--clientid":
                            configuration.ClientId = args[++i];
                            break;

                        case "--concurrency":
                            configuration.MaximumConcurrency = int.Parse(args[++i]);
                            break;

                        case "--send-buffer":
                            configuration.SendBufferSize = int.Parse(args[++i]);
                            break;

                        case "--receive-buffer":
                            configuration.ReceiveBufferSize = int.Parse(args[++i]);
                            break;

                        case "--timeout":
                            configuration.RequestTimeoutMs = int.Parse(args[++i]);
                            break;
                    }
                }
                // Minimal error management
                if (args.Length < 1 || !seeds || !topics)
                    throw new ArgumentException();
            }
            catch
            {
                // Minimal error management
                Console.WriteLine("Syntax error in option {0}", curOpt);
                Usage();
                Environment.Exit(-1);
            }

            var cluster =
                new Cluster(configuration, new ConsoleLogger());

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
