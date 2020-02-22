namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class Transaction : Event
    {
        public Transaction()
        {
            Events =  new List<Event>();
        }

        public string TransactionState { get; set; }
        public List<Event> Events { get; set; }
    }
}
