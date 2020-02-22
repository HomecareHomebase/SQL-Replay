namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class BulkInsert : Event
    {
        public string Table { get; set; }

        public List<Column> Columns { get; set; } = new List<Column>();

        public int Rows { get; set; }
        public bool FireTriggers { get; set; }

        public string BatchText { get; set; }
    }
}
