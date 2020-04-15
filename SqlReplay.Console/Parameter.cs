namespace SqlReplay.Console
{
    using System;
    using System.Data;

    [Serializable]
    public class Parameter
    {
        public string Name { get; set; }
        public SqlDbType SqlDbType { get; set; }        
        public int Size { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }
        public ParameterDirection Direction { get; set; }
        public string TypeName { get; set; }
        public int? UserTypeId { get; set; }
        public object Value { get; set; }
    }
}
