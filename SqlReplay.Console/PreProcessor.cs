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

    internal class PreProcessor
    {
        private Dictionary<string, Dictionary<string, Parameter>> procedureParameters = new Dictionary<string, Dictionary<string, Parameter>>();

        internal async Task<Run> PreProcess(string[] fileNames, string connectionString, DateTimeOffset? cutoff)
        {
            var sessions = new ConcurrentDictionary<string, Session>();

            using (var con = new SqlConnection(connectionString))
            {
                await con.OpenAsync();
                foreach (string fileName in fileNames)
                {                    
                    var xeStream = new XEFileEventStreamer(fileName);
                    await xeStream.ReadEventStream(xevent =>
                    {
                        if (xevent.Actions["database_name"].ToString() != con.Database)
                        {
                            return Task.CompletedTask;
                        }

                        if (cutoff != null && xevent.Timestamp > cutoff)
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
                                EventSequence = xevent.Actions["event_sequence"].ToString(),
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
                                    EventSequence = xevent.Actions["event_sequence"].ToString(),
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
                                EventSequence = xevent.Actions["event_sequence"].ToString(),
                                TransactionId = xevent.Actions["transaction_id"].ToString(),
                                BatchText = xevent.Fields["batch_text"].ToString(),
                                Timestamp = xevent.Timestamp
                            };

                            bulkInsert.Table = bulkInsert.BatchText.Split(' ')[2];
                            string[] columns = bulkInsert.BatchText.GetParenthesesContent().Split(", ");
                            foreach (string col in columns)
                            {
                                string[] columnInfo = col.Split(' ');
                                bulkInsert.Columns.Add(new Column {Name = columnInfo[0], DataType = columnInfo[1]});
                            }

                            if (bulkInsert.BatchText.Contains(" with ("))
                            {
                                string[] settings = bulkInsert.BatchText
                                    .GetParenthesesContent(bulkInsert.BatchText.IndexOf(" with (") + 6).Split(", ");
                                foreach (string setting in settings)
                                {
                                    if (setting == "FIRE_TRIGGERS")
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
                            else
                            {                                
                                var bulkInsert = (BulkInsert) session.Events
                                    .FirstOrDefault(e =>
                                        (e as BulkInsert)?.TransactionId ==
                                        xevent.Actions["transaction_id"].ToString() &&
                                        (e as BulkInsert)?.BatchText == xevent.Fields["batch_text"].ToString());
                                if (bulkInsert != null)
                                {
                                    bulkInsert.Rows = int.Parse(xevent.Fields["row_count"].ToString());
                                }
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
                session.Events = session.Events.OrderBy(e => e.EventSequence).ToList();
            }

            var run = new Run()
            {                
                Sessions = sessions.Values.ToArray().Where(s => s.Events.Count > 0).OrderBy(s => s.Events.First().EventSequence).ToList()
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
                select [Name]=[name], [Type]=type_name(user_type_id), [Length]=max_length, [Precision]=[precision], [Scale]=[scale], IsOutput=is_output
                from sys.all_parameters
                where object_id = object_id(@procedure)
                order by parameter_id", con))
                {
                    cmd.Parameters.Add(new SqlParameter("@procedure", SqlDbType.VarChar, 128) { Value = rpc.Procedure });
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var parameter = reader.GetString(0);
                            string rawSqlDbType = reader.GetString(1);
                            var size = (int)reader.GetInt16(2);
                            var precision = reader.GetByte(3);
                            var scale = reader.GetByte(4);
                            var isOutput = reader.GetBoolean(5);
                            SqlDbType sqlDbType;
                            if (rawSqlDbType == "sysname")
                            {
                                sqlDbType = SqlDbType.NVarChar;
                            }
                            else if (rawSqlDbType == "numeric")
                            {
                                sqlDbType = SqlDbType.Decimal;
                            }
                            else if (!Enum.TryParse(rawSqlDbType, true, out sqlDbType))
                            {
                                sqlDbType = SqlDbType.Udt;
                            }

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
                                    DbType = sqlDbType,
                                    Size = size,
                                    Precision = precision,
                                    Scale = scale,
                                    Direction = (isOutput) ? ParameterDirection.Output : ParameterDirection.Input
                                });
                        }
                    }
                }
                this.procedureParameters.Add(rpc.Procedure, parameters);
            }

            //Throw back anything that has a UDT parameter if we are dealing with any Statement besides a basic exec
            if (!rpc.Statement.StartsWith("exec ") && parameters.Any(p => p.Value.DbType == SqlDbType.Udt))
            {
                //Set Procedure back to null since we want to just execute the Statement as CommandText (SQLBatch) rather than execute as StoredProcedure with parameters (RPC)
                rpc.Procedure = null;
                return;
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
                    DbType = dictParam.DbType,
                    Size = dictParam.Size,
                    Precision = dictParam.Precision,
                    Scale = dictParam.Scale,
                    Direction = dictParam.Direction
                };
                if (value != "NULL" && value != "default")
                {
                    if (value.Contains('\''))
                    {
                        var start = value.IndexOf('\'') + 1;
                        var stringValue = value.Substring(start, value.Length - start - 1);
                        if (rpcParam.DbType == SqlDbType.UniqueIdentifier)
                        {
                            rpcParam.Value = Guid.Parse(stringValue);
                        }
                        else
                        {
                            rpcParam.Value = stringValue;
                        }
                    }
                    else if (!(value.StartsWith('@') || value.EndsWith(" output")))
                    {
                        switch (rpcParam.DbType)
                        {
                            case SqlDbType.BigInt:
                                rpcParam.Value = long.Parse(value);
                                break;
                            case SqlDbType.Bit:
                                rpcParam.Value = Convert.ToBoolean(byte.Parse(value));
                                break;
                            case SqlDbType.Char:
                            case SqlDbType.NChar:
                            case SqlDbType.VarChar:
                            case SqlDbType.NVarChar:
                            case SqlDbType.Text:
                            case SqlDbType.NText:
                                rpcParam.Value = value;
                                break;
                            case SqlDbType.Decimal:
                            case SqlDbType.Money:
                            case SqlDbType.SmallMoney:
                                rpcParam.Value = decimal.Parse(value);
                                break;
                            case SqlDbType.Float:
                                rpcParam.Value = double.Parse(value);
                                break;
                            case SqlDbType.Int:
                                rpcParam.Value = decimal.ToInt32(decimal.Parse(value));
                                break;
                            case SqlDbType.Real:
                                rpcParam.Value = float.Parse(value);
                                break;
                            case SqlDbType.UniqueIdentifier:
                                rpcParam.Value = Guid.Parse(value);
                                break;
                            case SqlDbType.SmallInt:
                                rpcParam.Value = short.Parse(value);
                                break;
                            case SqlDbType.TinyInt:
                                rpcParam.Value = byte.Parse(value);
                                break;
                            case SqlDbType.SmallDateTime:
                            case SqlDbType.DateTime:
                            case SqlDbType.Date:
                            case SqlDbType.Time:
                            case SqlDbType.DateTime2:
                                rpcParam.Value = DateTime.Parse(value);
                                break;
                            case SqlDbType.DateTimeOffset:
                                rpcParam.Value = DateTimeOffset.Parse(value);
                                break;
                            case SqlDbType.Binary:
                            case SqlDbType.Image:
                            case SqlDbType.Timestamp:
                            case SqlDbType.VarBinary:
                                rpcParam.Value = Encoding.UTF8.GetBytes(value);
                                break;
                            default:
                                throw new Exception(rpcParam.DbType + " is not a supported SqlDbType");                                
                        }
                    }
                    else if (value.StartsWith('@') && !value.EndsWith(" output"))
                    {
                        if (rpcParam.DbType == SqlDbType.Xml)
                        {
                            string xmlConvert = rpc.Statement.Substring(rpc.Statement.IndexOf($"set {value}=", StringComparison.CurrentCulture)).GetParenthesesContent();
                            int xmlBegin = xmlConvert.IndexOf('\'');
                            string xmlContent = xmlConvert.Substring(xmlBegin + 1, xmlConvert.Length - xmlBegin - 2);
                            rpcParam.Value = xmlContent;// new SqlXml(new MemoryStream(Encoding.UTF8.GetBytes(xmlContent)));
                        }
                        else
                        {
                            throw new Exception(rpcParam.DbType + " is not a supported SqlDbType for a non-output parameter whose value is set with a variable");
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
    }
}
