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
        
        public TimestampedException(Exception ex, string commandText)
        {
            UtcTimestamp = DateTime.UtcNow;
            Exception = ex;
            CommandText = commandText;
        }

        public DateTime UtcTimestamp { get; private set; }

        public Exception Exception { get; private set; }
        
        public string CommandText { get; private set; }

        public override string ToString()
        {
            var s = $"{GetTimestamp()} -\r\n{Exception}";
            s += string.IsNullOrEmpty(CommandText) ? "" : $"\r\nSQL text for above error:\r\n{CommandText}";
            return s;
        }

        private string GetTimestamp()
        {
            return string.Format($"{UtcTimestamp:MM-dd-yyyy--HH-mm-ss}");
        }
    }
}
