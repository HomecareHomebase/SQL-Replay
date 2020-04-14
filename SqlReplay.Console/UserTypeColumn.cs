namespace SqlReplay.Console
{
    using System;
    using System.Data;

    [Serializable]
    public class UserTypeColumn
    {
        public string Name { get; set; }
        public SqlDbType SqlDbType { get; set; }
        public int Size { get; set; }
    }
}
