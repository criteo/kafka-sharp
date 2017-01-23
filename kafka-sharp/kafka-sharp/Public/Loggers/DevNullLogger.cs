namespace Kafka.Public.Loggers
{
    public class DevNullLogger : ILogger
    {
        public void LogInformation(string message)
        {
        }

        public void LogWarning(string message)
        {
        }

        public void LogError(string message)
        {
        }

        public void LogDebug(string message)
        {
        }
    }
}