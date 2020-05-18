namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class BulkInsert : Event
    {
        public string Table { get; set; }

        public List<Column> Columns { get; set; } = new List<Column>();

        public List<List<object>> Rows { get; set; } = new List<List<object>>();

        public bool CheckConstraints { get; set; }

        public bool FireTriggers { get; set; }

        [NonSerialized] private string _batchText;
        public string BatchText
        {
            get => _batchText;
            set => _batchText = value;
        }

        [NonSerialized] private int _rowCount;
        public int RowCount
        {
            get => _rowCount;
            set => _rowCount = value;
        }
    }

}
