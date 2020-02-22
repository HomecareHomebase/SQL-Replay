namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class Run
    {
        public string ConnectionString { get; set; }
        public DateTimeOffset EventCaptureOrigin { get; set; }
        public List<Session> Sessions { get; set; }
    }
}
