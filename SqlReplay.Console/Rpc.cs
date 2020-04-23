namespace SqlReplay.Console
{
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;

    [Serializable, JsonObject(MemberSerialization.OptIn)]
    public class Rpc : Event
    {
        public string Statement { get; set; }
        public string ObjectName { get; set; }
        [JsonProperty("sp")]
        public string Procedure { get; set; }
        [JsonProperty("p")]
        public List<Parameter> Parameters { get; set; } = new List<Parameter>();
    }
}
