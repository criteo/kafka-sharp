using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Management;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.ITest
{
    internal class LocalZookeeper
    {
        public readonly int ListenPort;

        public readonly string ListenHost;

        public readonly string DataDir;

        public readonly string PropertiesFileName;

        public readonly string PropertiesFile;

        public readonly string Url;

        public LocalZookeeper(string rootDir, int listenPort = 2181, string listenHost = "127.0.0.1")
        {
            this.ListenPort = listenPort;
            this.ListenHost = listenHost;
            this.DataDir = new Uri(Path.Combine(rootDir, string.Format("zookeeper{0}", listenPort))).AbsolutePath;
            this.PropertiesFileName = string.Format("zookeeper{0}.properties", listenPort);
            this.PropertiesFile = Path.Combine(rootDir, PropertiesFileName);
            this.Url = string.Format("{0}:{1}", listenHost, listenPort);
        }

        public string PropertiesContent
        {
            get
            {
                return string.Format(@"dataDir={0}
clientPort={1}
maxClientCnxns=0
", this.DataDir, this.ListenPort);
            }
        }
    }

    internal class LocalBroker
    {
        public readonly int BrokerId;

        public readonly int ListenPort;

        public const string ListenHost = "127.0.0.1";

        public readonly string PropertiesFileName;

        public readonly string PropertiesFile;

        public readonly string LogsDir;

        public readonly string ZookeeperUrl;

        public Process Process;

        public LocalBroker(string rootDir, int brokerId, string zookeeperUrl = "127.0.0.1:2181")
        {
            this.BrokerId = brokerId;
            this.ListenPort = 9092 + brokerId;
            this.PropertiesFileName = string.Format("broker{0}.properties", this.BrokerId);
            this.PropertiesFile = Path.Combine(rootDir, PropertiesFileName);
            this.LogsDir = new Uri(Path.Combine(rootDir, string.Format("kafka-logs{0}", brokerId))).AbsolutePath;
            this.ZookeeperUrl = zookeeperUrl;
            Process = null;
        }

        public string PropertiesContent
        {
            get
            {
                return string.Format(@"broker.id={0}
port={1}
host.name={2}
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs={3}
num.partitions=1
num.recovery.threads.per.data.dir=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
log.cleaner.enable=false
zookeeper.connect={4}
zookeeper.connection.timeout.ms=6000
", this.BrokerId, this.ListenPort, LocalBroker.ListenHost, this.LogsDir, this.ZookeeperUrl);
            }
        }
    }

    internal class LocalKafkaCommander : IDisposable
    {
        public readonly string CommanderPath;

        public readonly string KafkaRootPath;

        public readonly LocalZookeeper Zookeeper;

        public LocalKafkaCommander(string kafkaRootPath)
        {
            this.CommanderPath = Path.Combine(Path.GetTempPath(), "kafkatest-" + Path.GetRandomFileName());
            Directory.CreateDirectory(this.CommanderPath);

            this.KafkaRootPath = kafkaRootPath;
            this.Zookeeper = new LocalZookeeper(this.CommanderPath);
            this.WriteFile(Zookeeper.PropertiesFile, Zookeeper.PropertiesContent);
        }

        public void Dispose()
        {
            this.StopZookeeper();
            Thread.Sleep(TimeSpan.FromSeconds(2));
            if (Directory.Exists(this.CommanderPath))
            {
                Directory.Delete(this.CommanderPath, true);
            }
        }

        public void StartZookeeper()
        {
            var starter = Shell.LaunchBat(this.KafkaRootPath,
                string.Format("zookeeper-server-start.bat {0}", this.Zookeeper.PropertiesFile),
                Drop,
                Drop,
                exitCode => Console.WriteLine("Zookeeper starter exited with {0}", exitCode));
        }

        public void StopZookeeper()
        {
            var stopper = Shell.LaunchBat(this.KafkaRootPath,
                string.Format("zookeeper-server-stop.bat {0}", this.Zookeeper.PropertiesFile),
                Drop,
                Drop,
                exitCode => Console.WriteLine("Zookeeper stopper exited with {0}", exitCode));
        }

        public LocalBroker CreateBroker(int brokerId)
        {
            var broker = new LocalBroker(this.CommanderPath, brokerId);
            this.WriteFile(broker.PropertiesFile, broker.PropertiesContent);
            return broker;
        }

        private void WriteFile(string filename, string content)
        {
            using (var file = File.CreateText(filename))
            {
                file.Write(content);
            }
        }

        private static void Drop(string toBeDropped)
        {
            // Console.WriteLine(toBeDropped);
        }

        public async Task StartBroker(LocalBroker broker)
        {
            Console.WriteLine("Start broker {0}", broker.BrokerId);
            broker.Process = Shell.LaunchBat(this.KafkaRootPath, string.Format("kafka-server-start.bat {0}", broker.PropertiesFile),
               Drop, //s => Console.WriteLine("BROKER {0} : {1}", broker.BrokerId, s),
               Drop, //s => Console.WriteLine("ERR BROKER {0} : {1}", broker.BrokerId, s),
               i => Console.WriteLine("StartBroker {0} exited with: {1}", broker.BrokerId, i));

            var timeout = Task.Delay(TimeSpan.FromSeconds(10));
            var res = await
                 Task.WhenAny(
                     new[]
                    {
                        new TcpClient().ConnectAsync(LocalBroker.ListenHost, broker.ListenPort),
                        timeout
                    });
            if (res == timeout)
            {
                throw new Exception(string.Format("Broker {0} doesn't seem to be started on {1}", broker.BrokerId, broker.ListenPort));
            }
        }

        public void CreateTopic(string topic, int nbPartitions, int replication)
        {
            var process = Shell.LaunchBat(
                this.KafkaRootPath,
                string.Format(
                    "kafka-topics.bat --create --topic {0} --partitions {1} --replication-factor {2} --zookeeper {3}",
                    topic,
                    nbPartitions,
                    replication,
                    Zookeeper.Url),
                Drop,
                Drop,
                i => Console.WriteLine("Create Topic exited with {0}", i));
            process.WaitForExit(10000);
            Thread.Sleep(TimeSpan.FromSeconds(1));
        }

        public void DeleteTopic(string topic)
        {
            Shell.LaunchBat(
                this.KafkaRootPath,
                string.Format(
                    "kafka-topics.bat --delete --topic {0} --zookeeper {1}",
                    topic,
                    Zookeeper.Url),
                Drop,
                Drop,
                i => Console.WriteLine("Delete Topic exited with {0}", i));

        }

        public void StopBroker(LocalBroker broker)
        {
            Console.WriteLine("Stop broker {0}", broker.BrokerId);
            //Shell.LaunchBat(this.KafkaRootPath, string.Format("kafka-server-stop.bat {0}", broker.PropertiesFile),
            //    Drop,
            //    Drop,
            //    i => Console.WriteLine("StopBroker {0} exited with: {1}", broker.BrokerId, i));
            if (broker.Process == null) return;
            Shell.KillProcessAndChildren(broker.Process);
            broker.Process = null;
        }

        public BatchConsumer StartConsumer(string topic, bool fromBeginning = true)
        {
            var logFile = Path.Combine(this.CommanderPath, string.Format("consumed-{0}-{1}", topic, Path.GetRandomFileName()));
            var process = Shell.LaunchBat(
                this.KafkaRootPath,
                string.Format("kafka-console-consumer.bat --zookeeper {0} --topic {1} {2} 1> {3}", this.Zookeeper.Url, topic, fromBeginning ? "--from-beginning" : "", logFile),
                Drop, //s => Console.WriteLine("CONSUMER : {0}", s),
                Drop, //s => Console.WriteLine("ERR CONSUMER : {0}", s),
                i => Console.WriteLine("ConsumerStarter exited with {0}", i));

            return new BatchConsumer(logFile, process);
        }

        public IEnumerable<string> StopConsumer(BatchConsumer consumer)
        {
            Thread.Sleep(TimeSpan.FromSeconds(8));
            Shell.KillProcessAndChildren(consumer.Process);
            var received = new List<string>();
            using (var recFile = File.Open(consumer.LogFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
            {
                using (var reader = new StreamReader(recFile))
                {
                    while (true)
                    {
                        var line = reader.ReadLine();
                        if (line == null)
                        {
                            break;
                        }
                        received.Add(line);
                    }
                }
            }
            return received;
        }
    }

    internal class BatchConsumer
    {
        public readonly string LogFile;

        public readonly Process Process;

        public BatchConsumer(string logFile, Process process)
        {
            this.LogFile = logFile;
            this.Process = process;
        }
    }

    internal static class Shell
    {
        internal static Process LaunchBat(
            string path,
            string file,
            Action<string> stdout,
            Action<string> stderr,
            Action<int> onExit)
        {
            Console.WriteLine("[{0}]EXEC {1}", DateTime.UtcNow, file);
            var pi = new ProcessStartInfo(@"cmd.exe", "/c " + file)
            {
                CreateNoWindow = true,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                WorkingDirectory = path
            };

            var p = Process.Start(pi);
            p.OutputDataReceived += (sender, args) => stdout(args.Data);
            p.ErrorDataReceived += (sender, args) => stderr(args.Data);
            p.Exited += (sender, args) => onExit(p.ExitCode);
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            return p;
        }

        public static void KillProcessAndChildren(Process process)
        {
            KillProcessAndChildren(process.Id);
        }

        private static void KillProcessAndChildren(int pid)
        {
            var searcher = new ManagementObjectSearcher
              ("Select * From Win32_Process Where ParentProcessID=" + pid);
            ManagementObjectCollection moc = searcher.Get();
            foreach (var mo in moc)
            {
                KillProcessAndChildren(Convert.ToInt32(mo["ProcessID"]));
            }
            try
            {
                Process proc = Process.GetProcessById(pid);
                proc.Kill();
            }
            catch (Exception ex)
            {
                // Process already exited.
            }
        }
    }
}