namespace SqlReplay.Console
{
    using Newtonsoft.Json;
    using System;
    using System.Data;

    [Serializable, JsonObject(MemberSerialization.OptIn)]
    public class Parameter
    {
        [JsonProperty("n")]
        public string Name { get; set; }
        public SqlDbType SqlDbType { get; set; }        
        public int Size { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }
        public ParameterDirection Direction { get; set; }
        public string TypeName { get; set; }
        public int? UserTypeId { get; set; }
        [JsonProperty("v")]
        public object Value { get; set; }
    }
}
