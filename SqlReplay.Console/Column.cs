namespace SqlReplay.Console
{
    using System;
    using System.Data;

    [Serializable]
    public class Column
    {
        public string Name { get; set; }
        public SqlDbType SqlDbType { get; set; }
        public int Size { get; set; }
    }
}
