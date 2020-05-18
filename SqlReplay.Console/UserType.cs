namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class UserType
    {
        public List<Column> Columns { get; set; } = new List<Column>();
        public List<List<object>> Rows { get; set; } = new List<List<object>>();
    }
}
