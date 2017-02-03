using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Public.Loggers
{
    public class ConsoleLogger : ILogger
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

        public void LogDebug(string message)
        {
            LogHeader("DEBUG");
            Console.WriteLine(message);
        }
    }
}
