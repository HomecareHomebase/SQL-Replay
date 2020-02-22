namespace SqlReplay.Console
{
    using System;

    [Serializable]
    public class Column
    {
        public string Name { get; set; }
        public string DataType { get; set; }
    }
}
