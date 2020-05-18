namespace SqlReplay.Console
{
    using Microsoft.SqlServer.XEvent.XELite;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Concurrent;
    using System.Linq;
    using System;
    using System.Text.RegularExpressions;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Text;
    using System.IO;

    internal class PreProcessor
    {
        private Dictionary<string, Dictionary<string, Parameter>> procedureParameters = new Dictionary<string, Dictionary<string, Parameter>>();
        private Dictionary<int, List<Column>> userTypeColumnDefinitions = new Dictionary<int, List<Column>>();

        internal async Task<Run> PreProcess(string[] filePaths, string connectionString)
        {
            var sessions = new ConcurrentDictionary<string, Session>();

            using (var con = new SqlConnection(connectionString))
            {
                await con.OpenAsync();
                foreach (string filePath in filePaths)
                {
                    if (!Regex.IsMatch(Path.GetFileName(filePath), @"^\w+\.xel$", RegexOptions.IgnoreCase))
                    {
                        //ignore any files that don't fit the pattern of an XE file
                        continue;
                    }
                    var xeStream = new XEFileEventStreamer(filePath);
                    await xeStream.ReadEventStream(xevent =>
                    {
                        if (xevent.Actions["database_name"].ToString() != con.Database)
                        {
                            return Task.CompletedTask;
                        }
                        Event evt = null;
                        if (xevent.Name == "rpc_starting" &&
                            xevent.Fields["object_name"].ToString() != "sp_reset_connection" &&
                            !xevent.Fields["statement"].ToString().StartsWith("exec sp_unprepare "))
                        {
                            evt = new Rpc()
                            {
                                EventSequence = long.Parse(xevent.Actions["event_sequence"].ToString()),
                                TransactionId = xevent.Actions["transaction_id"].ToString(),
                                Statement = xevent.Fields["statement"].ToString(),
                                ObjectName = xevent.Fields["object_name"].ToString(),
                                Timestamp = xevent.Timestamp
                            };
                            //Build parameters so we can replay statements as ADO.NET CommandType.StoredProcedure calls in order to avoid extra compilations of raw statement
                            LoadParameters(con, (Rpc)evt);
                        }
                        else if (xevent.Name == "sql_transaction")
                        {
                            if (xevent.Fields["transaction_type"].ToString() == "User")
                            {
                                evt = new Transaction()
                                {
                                    EventSequence = long.Parse(xevent.Actions["event_sequence"].ToString()),
                                    TransactionId = xevent.Fields["transaction_id"].ToString(),
                                    TransactionState = xevent.Fields["transaction_state"].ToString(),
                                    Timestamp = xevent.Timestamp
                                };
                            }
                        }
                        else if (xevent.Name == "sql_batch_starting" &&
                                 xevent.Fields["batch_text"].ToString().Contains("insert bulk"))
                        {
                            var bulkInsert = new BulkInsert()
                            {
                                EventSequence = long.Parse(xevent.Actions["event_sequence"].ToString()),
                                TransactionId = xevent.Actions["transaction_id"].ToString(),
                                BatchText = xevent.Fields["batch_text"].ToString(),
                                Timestamp = xevent.Timestamp
                            };

                            bulkInsert.Table = bulkInsert.BatchText.Split(' ')[2];
                            string[] columns = bulkInsert.BatchText.GetParenthesesContent().Split(", ");
                            foreach (string col in columns)
                            {
                                string[] columnInfo = col.Split(' ');
                                bulkInsert.Columns.Add(new Column {Name = columnInfo[0], SqlDbType = GetSqlDbType(columnInfo[1])});
                            }

                            if (bulkInsert.BatchText.Contains(" with ("))
                            {
                                string[] settings = bulkInsert.BatchText
                                    .GetParenthesesContent(bulkInsert.BatchText.IndexOf(" with (") + 6).Split(", ");
                                foreach (string setting in settings)
                                {
                                    if (setting == "CHECK_CONSTRAINTS")
                                    {
                                        bulkInsert.CheckConstraints = true;
                                    }
                                    else if (setting == "FIRE_TRIGGERS")
                                    {
                                        bulkInsert.FireTriggers = true;
                                        break;
                                    }
                                }
                            }

                            evt = bulkInsert;
                        }
                        else if (xevent.Name == "sql_batch_completed" &&
                                 xevent.Fields["batch_text"].ToString().Contains("insert bulk"))
                        {
                            if (!sessions.TryGetValue(xevent.Actions["session_id"].ToString(), out var session))
                            {
                                throw new Exception(
                                    $"Could not find session ID {xevent.Actions["session_id"].ToString()} for bulk insert.");
                            }
                            var bulkInsert = (BulkInsert) session.Events
                                .FirstOrDefault(e =>
                                    (e as BulkInsert)?.TransactionId ==
                                    xevent.Actions["transaction_id"].ToString() &&
                                    (e as BulkInsert)?.BatchText == xevent.Fields["batch_text"].ToString());
                            if (bulkInsert != null)
                            {
                                bulkInsert.RowCount = int.Parse(xevent.Fields["row_count"].ToString());
                                AddBulkInsertData(bulkInsert, con);
                            }
                        }

                        if (evt != null)
                        {
                            string sessionId = xevent.Actions["session_id"].ToString();
                            Session session = sessions.GetOrAdd(sessionId, new Session() {SessionId = sessionId});
                            session.Events.Add(evt);                            
                        }                        
                        return Task.CompletedTask;
                    }, CancellationToken.None);
                }
            }

            foreach (Session session in sessions.Values)
            {
                //Remove any bulk inserts where we never found a corresponding sql_batch_completed
                session.Events.RemoveAll(e => (e as BulkInsert)?.RowCount == 0);
                session.Events = session.Events.OrderBy(e => e.EventSequence).ToList();
            }

            var run = new Run()
            {                
                Sessions = sessions.Values.ToArray().Where(s => s.Events.Count > 0).OrderBy(s => s.Events.First().Timestamp).ToList()
            };
            run.EventCaptureOrigin = run.Sessions.First().Events.First().Timestamp;
            return run;
        }

        internal void LoadParameters(SqlConnection con, Rpc rpc)
        {
            //We are not handling creating parameters for dynamic SQL or anything without named parameters - these will be executed just as unparameterized statements
            if (rpc.ObjectName == "sp_prepexec" || rpc.ObjectName == "sp_prepexecrpc" || rpc.ObjectName == "sp_executesql" ||
                (rpc.Statement.IndexOf('\'') >= 0 && rpc.Statement.IndexOf('\'') < rpc.Statement.IndexOf('@')) ||
                (rpc.Statement.IndexOf('=') >= 0 && rpc.Statement.IndexOf('=') < rpc.Statement.IndexOf('@')) ||
                (rpc.Statement.Contains(',') && !rpc.Statement.Contains('@')) ||
                (!rpc.Statement.Contains('@') && Regex.IsMatch(rpc.Statement, @" [0-9]")))
            {
                return;
            }          

            string execStatement;
            if (!rpc.Statement.StartsWith("exec "))
            {
                int execStart = rpc.Statement.IndexOf("\r\nexec") + 2;
                int execEnd = rpc.Statement.IndexOf("\r\nselect", execStart);
                if (execEnd < 0)
                {
                    execStatement = rpc.Statement.Substring(execStart);
                }
                else
                {
                    execStatement = rpc.Statement.Substring(execStart, execEnd - execStart);
                }
            }
            else
            {
                execStatement = rpc.Statement;
            }
                
            //ObjectName will never include the schema but the Statement will when we need it, so let's parse the procedure from there
            rpc.Procedure = execStatement.Substring(5, (execStatement.Contains('@') ? execStatement.IndexOf('@') : execStatement.Length) - 5).Trim();

            if (!this.procedureParameters.TryGetValue(rpc.Procedure, out var parameters))
            {
                parameters = new Dictionary<string, Parameter>();
                using (var cmd = new SqlCommand(@"
                select [Name]=p.[name], [Type]=type_name(p.system_type_id), [Length]=p.max_length, [Precision]=p.[precision], [Scale]=p.[scale], IsOutput=p.is_output, TypeName=schema_name(tt.schema_id) + '.' + type_name(tt.user_type_id), UserTypeId=tt.user_type_id
                from sys.all_parameters p
                left join sys.table_types tt on tt.user_type_id = p.user_type_id
                where object_id = object_id(@procedure)
                order by parameter_id", con))
                {
                    cmd.Parameters.Add(new SqlParameter("@procedure", SqlDbType.VarChar, 128) { Value = rpc.Procedure });
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var parameter = reader.GetString(0);
                            var rawSqlDbType = reader.GetString(1);                            
                            var size = (int)reader.GetInt16(2);
                            var precision = reader.GetByte(3);
                            var scale = reader.GetByte(4);
                            var isOutput = reader.GetBoolean(5);
                            var typeName = (!reader.IsDBNull(6)) ? reader.GetString(6) : null;
                            var userTypeId = (!reader.IsDBNull(7)) ? (int?)reader.GetInt32(7) : null;
                            var sqlDbType = GetSqlDbType(rawSqlDbType);                            

                            if (string.IsNullOrWhiteSpace(parameter) && isOutput)
                            {
                                //user-defined functions have a built-in output parameter with no name that we do not want to process
                                continue;
                            }
                            parameter = parameter.Substring(1);
                            parameters.Add(parameter.ToLower(),
                                new Parameter
                                {
                                    Name = parameter,
                                    SqlDbType = sqlDbType,                                    
                                    Size = size,
                                    Precision = precision,
                                    Scale = scale,
                                    Direction = (isOutput) ? ParameterDirection.Output : ParameterDirection.Input,
                                    TypeName = typeName,
                                    UserTypeId = userTypeId
                                });
                        }
                    }
                }
                this.procedureParameters.Add(rpc.Procedure, parameters);
            }

            //substitute ||| for commas as parameter delimiter due to potential for commas in strings
            var parameterString = Regex.Replace(execStatement.Substring(execStatement.IndexOf(rpc.Procedure, StringComparison.CurrentCulture) + rpc.Procedure.Length + 1), @",@[a-zA-Z0-9_]+ ?=", (m) => m.Value.Replace(",@", "|||@")).Trim();            
            if (string.IsNullOrEmpty(parameterString))
            {
                return;
            }
            var splits = parameterString.Split("|||");
            foreach (var s in splits)
            {
                var paramName = s.Substring(1, s.IndexOf('=') - 1);
                var value = s.Substring(s.IndexOf('=') + 1);
                var dictParam = parameters[paramName.Trim().ToLower()];
                
                var rpcParam = new Parameter
                {
                    Name = dictParam.Name,
                    SqlDbType = dictParam.SqlDbType,                    
                    Size = dictParam.Size,
                    Precision = dictParam.Precision,
                    Scale = dictParam.Scale,
                    Direction = dictParam.Direction,
                    TypeName = dictParam.TypeName,
                    UserTypeId = dictParam.UserTypeId
                };
                if (value != "NULL" && value != "default")
                {
                    if (value.Contains('\''))
                    {
                        rpcParam.Value = GetSystemValueFromSqlString(value, rpcParam.SqlDbType);
                    }
                    else if (!(value.StartsWith('@') || value.EndsWith(" output")))
                    {
                        rpcParam.Value = GetSystemValue(value, rpcParam.SqlDbType);
                    }
                    else if (value.StartsWith('@') && !value.EndsWith(" output"))
                    {
                        if (rpcParam.SqlDbType == SqlDbType.Xml)
                        {
                            string xmlConvert = rpc.Statement.Substring(rpc.Statement.IndexOf($"set {value}=", StringComparison.CurrentCulture)).GetParenthesesContent();
                            int xmlBegin = xmlConvert.IndexOf('\'');
                            string xmlContent = xmlConvert.Substring(xmlBegin + 1, xmlConvert.Length - xmlBegin - 2);
                            rpcParam.Value = xmlContent;// new SqlXml(new MemoryStream(Encoding.UTF8.GetBytes(xmlContent)));
                        }
                        else if (rpcParam.SqlDbType == SqlDbType.Structured)
                        {
                            if (!this.userTypeColumnDefinitions.TryGetValue((int)rpcParam.UserTypeId, out var userTypeColumns))
                            {
                                userTypeColumns = new List<Column>();

                                using (var cmd = new SqlCommand(@"
                                select [Name]=c.[name], [Type]=type_name(c.system_type_id), [Length]=c.max_length
                                from sys.table_types tt
                                join sys.columns c on c.object_id = tt.type_table_object_id
                                where tt.user_type_id = @userTypeId and c.is_identity = 0
                                order by c.column_id", con))
                                {
                                    cmd.Parameters.Add(new SqlParameter("@userTypeId", SqlDbType.Int) { Value = (int)rpcParam.UserTypeId });
                                    using (var reader = cmd.ExecuteReader())
                                    {
                                        while (reader.Read())
                                        {
                                            var name = reader.GetString(0);
                                            var rawSqlDbType = reader.GetString(1);
                                            var size = (int)reader.GetInt16(2);
                                            var sqlDbType = GetSqlDbType(rawSqlDbType);
                                            userTypeColumns.Add(new Column
                                            {
                                                Name = name,
                                                SqlDbType = sqlDbType,
                                                Size = size
                                            });
                                        }
                                    }
                                }
                                this.userTypeColumnDefinitions.Add((int)rpcParam.UserTypeId, userTypeColumns);
                            }

                            var tvpValue = new UserType { Columns = userTypeColumns };                               
                            var index = 0;
                            while (index >= 0)
                            {
                                index = rpc.Statement.IndexOf($"insert into {value} values", index, StringComparison.CurrentCulture);
                                if (index < 0)
                                {
                                    break;
                                }
                                //This works so long as one of the values doesn't have a right parethesis without a preceding left parenthesis
                                var insertedValuesString = rpc.Statement.Substring(index).GetParenthesesContent();
                                //split string by commas that have an even number of single quotes following them to avoid splitting on commas in values                            
                                var insertValues = Regex.Split(insertedValuesString, ",(?=(?:[^']*'[^']*')*[^']*$)");

                                var row = new List<object>();
                                for (var columnIndex = 0; columnIndex < tvpValue.Columns.Count; columnIndex++)
                                {
                                    if (insertValues[columnIndex] == "NULL" || insertValues[columnIndex] == "default")
                                    {
                                        row.Add(DBNull.Value);
                                    }
                                    else if (insertValues[columnIndex].Contains('\''))
                                    {
                                        row.Add(GetSystemValueFromSqlString(insertValues[columnIndex], tvpValue.Columns[columnIndex].SqlDbType));
                                    }
                                    else
                                    {
                                        row.Add(GetSystemValue(insertValues[columnIndex], tvpValue.Columns[columnIndex].SqlDbType));                                        
                                    }
                                }
                                tvpValue.Rows.Add(row);
                                index++;
                            }
                            rpcParam.Value = tvpValue;
                        }
                        else
                        {
                            throw new Exception(rpcParam.SqlDbType + " is not a supported SqlDbType for a non-output parameter whose value is set with a variable");
                        }
                    }
                }
                else
                {
                    rpcParam.Value = DBNull.Value;
                }
                rpc.Parameters.Add(rpcParam);
            }
        }

        private void AddBulkInsertData(BulkInsert bulkInsert, SqlConnection con)
        {
            using (DataTable dataTable = GetBulkInsertDataFromTable(bulkInsert, con))
            {
                for (var rowIndex = 0; rowIndex < bulkInsert.RowCount; rowIndex++)
                {
                    var row = new List<object>();
                    for (var columnIndex = 0; columnIndex < bulkInsert.Columns.Count; columnIndex++)
                    {
                        row.Add(dataTable.Rows[rowIndex][columnIndex]);
                    }
                    bulkInsert.Rows.Add(row);
                }
            }
        }

        private DataTable GetBulkInsertDataFromTable(BulkInsert bulkInsert, SqlConnection con)
        {
            var dataTable = new DataTable();
            var cmdText = $" select top(@count) {string.Join(',', bulkInsert.Columns.Select(c => c.Name).ToArray())} from {bulkInsert.Table}";
            using (var cmd = new SqlCommand(cmdText, con))
            {
                cmd.Parameters.Add(new SqlParameter("@count", SqlDbType.Int) { Value = bulkInsert.RowCount });
                using (var dataAdapter = new SqlDataAdapter(cmd))
                {
                    dataAdapter.Fill(dataTable);
                }
            }
            return dataTable;
        }
      
        private SqlDbType GetSqlDbType(string rawSqlDbType)
        {
            var leftParenthesisIndex = rawSqlDbType.IndexOf('(');
            var refinedSqlDbType = (leftParenthesisIndex < 0) ? rawSqlDbType : rawSqlDbType.Substring(0, leftParenthesisIndex);

            SqlDbType sqlDbType;
            if (refinedSqlDbType == "sysname")
            {
                sqlDbType = SqlDbType.NVarChar;
            }
            else if (refinedSqlDbType == "numeric")
            {
                sqlDbType = SqlDbType.Decimal;
            }
            else if (refinedSqlDbType == "table type")
            {
                sqlDbType = SqlDbType.Structured;
            }
            else if (!Enum.TryParse(refinedSqlDbType, true, out sqlDbType))
            {
                throw new Exception($"{refinedSqlDbType} could not be parsed into a SqlDbType");
            }
            return sqlDbType;
        }

        private object GetSystemValueFromSqlString(string value, SqlDbType sqlDbType)
        {
            var start = value.IndexOf('\'') + 1;
            var stringValue = value.Substring(start, value.Length - start - 1);
            if (sqlDbType == SqlDbType.UniqueIdentifier)
            {
                return Guid.Parse(stringValue);
            }
            else
            {
                return stringValue;
            }
        }

        private object GetSystemValue(string value, SqlDbType sqlDbType)
        {
            switch (sqlDbType)
            {
                case SqlDbType.BigInt:
                    return decimal.ToInt64(decimal.Parse(value));
                case SqlDbType.Bit:
                    return Convert.ToBoolean(decimal.ToByte(decimal.Parse(value)));
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.VarChar:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.NText:
                    return value;
                case SqlDbType.Decimal:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return decimal.Parse(value);
                case SqlDbType.Float:
                    return double.Parse(value);
                case SqlDbType.Int:
                    return decimal.ToInt32(decimal.Parse(value));
                case SqlDbType.Real:
                    return float.Parse(value);
                case SqlDbType.UniqueIdentifier:
                    return Guid.Parse(value);
                case SqlDbType.SmallInt:
                    return decimal.ToInt16(decimal.Parse(value));
                case SqlDbType.TinyInt:
                    return decimal.ToByte(decimal.Parse(value));
                case SqlDbType.SmallDateTime:
                case SqlDbType.DateTime:
                case SqlDbType.Date:
                case SqlDbType.Time:
                case SqlDbType.DateTime2:
                    return DateTime.Parse(value);
                case SqlDbType.DateTimeOffset:
                    return DateTimeOffset.Parse(value);
                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.Timestamp:
                case SqlDbType.VarBinary:
                    return Encoding.UTF8.GetBytes(value);
                default:
                    throw new Exception(sqlDbType + " is not a supported SqlDbType");
            }
        }
    }
}
