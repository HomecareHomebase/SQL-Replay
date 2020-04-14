namespace SqlReplay.Console
{
    using System;
    
    [Serializable]
    public abstract class Event
    {
        public long EventSequence { get; set; }
        public string TransactionId { get; set; }        
        public DateTimeOffset Timestamp { get; set; }
    }
}
