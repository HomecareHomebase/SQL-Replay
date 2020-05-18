namespace SqlReplay.Console
{
    using System;
    using System.Threading.Tasks;
    using System.Data.SqlClient;
    using System.Collections.Generic;
    using System.Linq;
    using System.Data;
    using Microsoft.SqlServer.Server;

    public class EventExecutor
    {       
        public List<Exception> Exceptions { get; set; } = new List<Exception>();

        public async Task ExecuteEvents(DateTimeOffset eventCaptureOrigin, DateTimeOffset replayOrigin, IEnumerable<Event> events, string connectionString)
        {
            List<Task> tasks = new List<Task>();
            foreach (var evt in events)
            {
                tasks.Add(Task.Run(async () =>
                {
                    TimeSpan timeToDelay = evt.Timestamp.Subtract(eventCaptureOrigin).Subtract(DateTimeOffset.UtcNow.Subtract(replayOrigin));
                    if (timeToDelay.TotalMilliseconds > 0)
                    {
                        await Task.Delay(timeToDelay);
                    }

                    if (evt is Transaction trx)
                    {
                        if (trx.TransactionState == "Begin")
                        {
                            var con = await GetOpenSqlConnectionAsync(connectionString);
                            if (con == null) { return; }
                            try
                            {
                                using (SqlTransaction dbTrx = con.BeginTransaction())
                                {
                                    foreach (var trxEvt in trx.Events)
                                    {
                                        TimeSpan timeToDelayTrxEvent = trxEvt.Timestamp.Subtract(eventCaptureOrigin).Subtract(DateTimeOffset.UtcNow.Subtract(replayOrigin));
                                        if (timeToDelayTrxEvent.TotalMilliseconds > 0)
                                        {
                                            await Task.Delay(timeToDelayTrxEvent);
                                        }

                                        if (trxEvt is Transaction transaction)
                                        {
                                            if (transaction.TransactionState == "Commit")
                                            {
                                                try
                                                {
                                                    dbTrx.Commit();
                                                }
                                                catch (Exception ex)
                                                {
                                                    this.Exceptions.Add(ex);
                                                }
                                                finally
                                                {
                                                    dbTrx.Connection?.Close();
                                                    dbTrx.Connection?.Dispose();
                                                    dbTrx.Dispose();
                                                }
                                            }
                                            else if (trx.TransactionState == "Rollback")
                                            {
                                                try
                                                {
                                                    dbTrx.Rollback();
                                                }
                                                catch (Exception ex)
                                                {
                                                    this.Exceptions.Add(ex);
                                                }
                                                finally
                                                {
                                                    dbTrx.Connection?.Close();
                                                    dbTrx.Connection?.Dispose();
                                                    dbTrx.Dispose();
                                                }
                                            }
                                        }
                                        else
                                        {
                                            if (trxEvt is Rpc rpc)
                                            {
                                                string commandText;
                                                CommandType commandType;
                                                if (!string.IsNullOrWhiteSpace(rpc.Procedure))
                                                {
                                                    commandText = rpc.Procedure;
                                                    commandType = CommandType.StoredProcedure;
                                                }
                                                else
                                                {
                                                    commandText = rpc.Statement;
                                                    commandType = CommandType.Text;
                                                }
                                                using (var cmd = new SqlCommand(commandText, dbTrx.Connection, dbTrx)
                                                {
                                                    CommandType = commandType,
                                                    CommandTimeout = 1800
                                                })
                                                {
                                                    SetupSqlCommandParameters(cmd, rpc);
                                                    try
                                                    {
                                                        cmd.ExecuteNonQuery();
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        this.Exceptions.Add(ex);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                this.Exceptions.Add(ex);
                            }
                            finally
                            {
                                con?.Close();
                                con?.Dispose();
                            }
                        }
                    }
                    else if (evt is Rpc rpc)
                    {
                        using (var con = await GetOpenSqlConnectionAsync(connectionString))
                        {
                            if (con == null) { return; }
                            string commandText;
                            CommandType commandType;
                            if (!string.IsNullOrWhiteSpace(rpc.Procedure))
                            {
                                commandText = rpc.Procedure;
                                commandType = CommandType.StoredProcedure;
                            }
                            else
                            {
                                commandText = rpc.Statement;
                                commandType = CommandType.Text;
                            }
                            using (var cmd = new SqlCommand(commandText, con)
                            {
                                CommandType = commandType,
                                CommandTimeout = 1800
                            })
                            {
                                SetupSqlCommandParameters(cmd, rpc);
                                try
                                {
                                    await cmd.ExecuteNonQueryAsync();
                                }
                                catch (Exception ex)
                                {
                                    this.Exceptions.Add(ex);
                                }
                            }
                            con.Close();
                        }
                    }
                    else if (evt is BulkInsert bulk)
                    {
                        if (bulk.Rows.Count == 0) { return; }
                        var dataTable = new DataTable();
                        foreach (var column in bulk.Columns)
                        {
                            dataTable.Columns.Add(GetDataColumn(column));
                        }

                        for (var rowIndex = 0; rowIndex < bulk.Rows.Count; rowIndex++)
                        {
                            DataRow dataRow = dataTable.NewRow();
                            for (var columIndex = 0; columIndex < bulk.Columns.Count; columIndex++)
                            {
                                dataRow[columIndex] = bulk.Rows[rowIndex][columIndex];
                            }
                            dataTable.Rows.Add(dataRow);
                        }

                        using (var con = await GetOpenSqlConnectionAsync(connectionString))
                        {
                            if (con == null) { return; }
                            SqlBulkCopyOptions options;
                            if (bulk.CheckConstraints && bulk.FireTriggers)
                            {
                                options = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers;
                            }
                            else if (bulk.CheckConstraints)
                            {
                                options = SqlBulkCopyOptions.CheckConstraints;
                            }
                            else if (bulk.FireTriggers)
                            {
                                options = SqlBulkCopyOptions.FireTriggers;
                            }
                            else
                            {
                                options = SqlBulkCopyOptions.Default;
                            }
                            try
                            {
                                using (var bulkCopy = new SqlBulkCopy(con, options, null) { BulkCopyTimeout = 1800 })
                                {
                                    bulkCopy.DestinationTableName = bulk.Table;
                                    foreach (DataColumn column in dataTable.Columns)
                                    {
                                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(column.ColumnName, column.ColumnName));
                                    }

                                    await bulkCopy.WriteToServerAsync(dataTable);
                                }
                            }
                            catch (Exception ex)
                            {
                                this.Exceptions.Add(ex);
                            }
                            con.Close();
                        }
                    }
                }));
            }
            await Task.WhenAll(tasks);
            Console.WriteLine("Ending bucket: " + events.First().Timestamp);
        }

        private async Task<SqlConnection> GetOpenSqlConnectionAsync(string connectionString)
        {
            var con = new SqlConnection(connectionString);
            try
            {
                await con.OpenAsync();
            }
            catch
            {
                try
                {
                    await con.OpenAsync();
                }
                catch(Exception)
                {
                    //Don't log failed connection attempts as these just muddy the exception logs
                    con.Dispose();
                    return null;
                }
            }
            return con;
        }

        private void SetupSqlCommandParameters(SqlCommand cmd, Rpc rpc)
        {
            foreach (var param in rpc.Parameters)
            {
                var sqlParam = new SqlParameter
                {
                    ParameterName = param.Name,
                    SqlDbType = param.SqlDbType,
                    Size = param.Size,
                    Precision = param.Precision,
                    Scale = param.Scale,
                    Direction = param.Direction
                };
                if (param.SqlDbType == SqlDbType.Structured)
                {
                    if (param.Value != DBNull.Value)
                    {
                        var userType = (UserType) param.Value;
                        if (userType.Rows.Count > 0)
                        {
                            var sqlMetaData = new SqlMetaData[userType.Columns.Count];
                            for (var i = 0; i < userType.Columns.Count; i++)
                            {
                                var col = userType.Columns[i];
                                switch (col.SqlDbType)
                                {
                                    case SqlDbType.Char:
                                    case SqlDbType.NChar:
                                    case SqlDbType.NVarChar:
                                    case SqlDbType.VarChar:
                                        sqlMetaData[i] = new SqlMetaData(col.Name, col.SqlDbType, col.Size);
                                        break;
                                    default:
                                        sqlMetaData[i] = new SqlMetaData(col.Name, col.SqlDbType);
                                        break;
                                }
                            }

                            var tvpValue = new List<SqlDataRecord>();
                            foreach (var row in userType.Rows)
                            {
                                var sqlDataRecord = new SqlDataRecord(sqlMetaData);
                                for (var i = 0; i < sqlMetaData.Length; i++)
                                {
                                    switch (sqlMetaData[i].SqlDbType)
                                    {
                                        case SqlDbType.SmallDateTime:
                                        case SqlDbType.DateTime:
                                        case SqlDbType.Date:
                                        case SqlDbType.Time:
                                        case SqlDbType.DateTime2:
                                            DateTime.TryParse(row[i].ToString(), out var dateTime);
                                            sqlDataRecord.SetValue(i, dateTime);
                                            break;
                                        case SqlDbType.DateTimeOffset:
                                            DateTimeOffset.TryParse(row[i].ToString(), out var dateTimeOffset);
                                            sqlDataRecord.SetValue(i, dateTimeOffset);
                                            break;
                                        default:
                                            sqlDataRecord.SetValue(i, row[i]);
                                            break;
                                    }
                                }

                                tvpValue.Add(sqlDataRecord);
                            }
                            sqlParam.Value = tvpValue;
                        }
                    }
                    sqlParam.TypeName = param.TypeName;
                }
                else
                {
                    sqlParam.Value = param.Value;
                }
                cmd.Parameters.Add(sqlParam);
            }
        }

        private DataColumn GetDataColumn(Column column)
        {
            string columnName = column.Name.Substring(1, column.Name.Length - 2);
            if (column.SqlDbType.ToString() == "BigInt")
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(long) };
            }
            else if (column.SqlDbType.ToString() == "SmallInt")
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(short) };
            }
            else if (column.SqlDbType.ToString() == "TinyInt")
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(byte) };
            }
            else if (column.SqlDbType.ToString() == "Int")
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(int) };
            }
            else if (column.SqlDbType.ToString().Contains("Text") || column.SqlDbType.ToString().Contains("Char"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(string) };
            }                    
            else if (column.SqlDbType.ToString() == "Bit")
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(bool) };
            }
            else if (column.SqlDbType.ToString().Contains("Date") || column.SqlDbType.ToString().Contains("Time"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(DateTime) };
            }
            else if (column.SqlDbType.ToString().Contains("Decimal") || column.SqlDbType.ToString().Contains("Money"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(decimal) };
            }
            else if (column.SqlDbType.ToString().Contains("Float"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(double) };
            }
            else
            {
                throw new Exception(column.SqlDbType + " is not a supported data type.");
            }
        }      
    }
}
