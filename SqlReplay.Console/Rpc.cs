namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class Rpc : Event
    {
        public string Statement { get; set; }
        public string ObjectName { get; set; }
        public string Procedure { get; set; }
        public List<Parameter> Parameters { get; set; } = new List<Parameter>();
    }
}
