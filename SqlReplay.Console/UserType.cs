namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class UserType
    {
        public List<UserTypeColumn> Columns { get; set; } = new List<UserTypeColumn>();
        public List<List<object>> Rows { get; set; } = new List<List<object>>();
    }
}
