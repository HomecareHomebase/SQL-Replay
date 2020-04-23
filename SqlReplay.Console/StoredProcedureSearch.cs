using System;
using System.Collections.Generic;
using System.Text;

namespace SqlReplay.Console
{
    public static class StoredProcedureSearch
    {
        public static List<string> CreateMatchCriteria(string[] storedProcedureNames)
        {
            var matchCriteria = new List<string>();
            foreach (var sp in storedProcedureNames)
            {
                //Look for schema and handle brackets, add all the variations to match criteria
                var storedProcedureName = sp.Replace("[", "").Replace("]", "");
                var parts = storedProcedureName.Split('.');
                if (parts.Length > 1)
                {
                    var schema = parts[0];
                    storedProcedureName = parts[1];
                    matchCriteria.Add($"{schema}.{storedProcedureName}");
                    matchCriteria.Add($"[{schema}].[{storedProcedureName}]");
                }
                matchCriteria.Add($"{storedProcedureName}");
                matchCriteria.Add($"[{storedProcedureName}]");
            }
            return matchCriteria;
        }
    }
}
