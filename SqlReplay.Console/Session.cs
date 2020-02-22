
namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class Session
    {
        public string SessionId { get; set; }
        public List<Event> Events { get; set; } = new List<Event>();
    }
}
