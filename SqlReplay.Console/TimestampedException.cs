using System;

namespace SqlReplay.Console
{
    [Serializable]
    public class TimestampedException
    {
        public TimestampedException(Exception ex)
        {
            UtcTimestamp = DateTime.UtcNow;
            Exception = ex;
        }

        public DateTime UtcTimestamp { get; private set; }

        public Exception Exception { get; private set; }

        public override string ToString()
        {
            return $"{GetTimestamp()} - {Exception}";
        }

        private string GetTimestamp()
        {
            return string.Format($"{UtcTimestamp:MM-dd-yyyy--HH-mm-ss}");
        }
    }
}
