namespace SqlReplay.Console
{
    using System;
    
    [Serializable]
    public abstract class Event
    {
        public string EventSequence { get; set; }
        public string TransactionId { get; set; }        
        public DateTimeOffset Timestamp { get; set; }
    }
}
