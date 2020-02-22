namespace SqlReplay.Console
{
    using System;
    using System.Threading.Tasks;
    using System.Data.SqlClient;
    using System.Collections.Generic;
    using System.Linq;
    using System.Data;

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
                            var con = await GetOpenSqlConnection(connectionString);
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
                                                    foreach (var param in rpc.Parameters)
                                                    {
                                                        cmd.Parameters.Add(new SqlParameter
                                                        {
                                                            ParameterName = param.Name,
                                                            SqlDbType = param.DbType,
                                                            Size = param.Size,
                                                            Precision = param.Precision,
                                                            Scale = param.Scale,
                                                            Direction = param.Direction,
                                                            Value = param.Value
                                                        });
                                                    }
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
                        using (var con = await GetOpenSqlConnection(connectionString))
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
                                foreach (var param in rpc.Parameters)
                                {
                                    cmd.Parameters.Add(new SqlParameter
                                    {
                                        ParameterName = param.Name,
                                        SqlDbType = param.DbType,
                                        Size = param.Size,
                                        Precision = param.Precision,
                                        Scale = param.Scale,
                                        Direction = param.Direction,
                                        Value = param.Value
                                    });
                                }
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
                        var dataTable = new DataTable();
                        foreach (Column column in bulk.Columns)
                        {
                            dataTable.Columns.Add(GetDataColumn(column));
                        }

                        for (int i = 0; i < bulk.Rows; ++i)
                        {                         
                            dataTable.Rows.Add(dataTable.NewRow());
                        }

                        using (var con = await GetOpenSqlConnection(connectionString))
                        {
                            if (con == null) { return; }
                            var options = (bulk.FireTriggers) ? SqlBulkCopyOptions.FireTriggers : SqlBulkCopyOptions.Default;
                            try
                            {
                                using (var bulkCopy = new SqlBulkCopy(con, options, null) { BulkCopyTimeout = 30 })
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

        private async Task<SqlConnection> GetOpenSqlConnection(string connectionString)
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
                catch(Exception ex)
                {
                    this.Exceptions.Add(ex);
                    con.Dispose();
                    return null;
                }
            }
            return con;
        }

        private DataColumn GetDataColumn(Column column)
        {
            string columnName = column.Name.Substring(1, column.Name.Length - 2);
            if (column.DataType.Contains("Int"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(int), DefaultValue = 255};
            }
            else if (column.DataType.Contains("Text") || column.DataType.Contains("(max)"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(string), DefaultValue = "X".Lengthen(8000) };
            }
            else if (column.DataType.Contains("Char"))
            {
                var charValue = "X".Lengthen(int.Parse(column.DataType.GetParenthesesContent()));
                return new DataColumn { ColumnName = columnName, DataType = typeof(string), DefaultValue = charValue};
            }            
            else if (column.DataType == "Bit")
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(bool), DefaultValue = true};
            }
            else if (column.DataType.Contains("Date"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(DateTime), DefaultValue = DateTime.UtcNow};
            }
            else if (column.DataType.Contains("Decimal") || column.DataType.Contains("Money"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(decimal), DefaultValue = 999.99m};
            }
            else if (column.DataType.Contains("Float"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(double), DefaultValue = 999.99d };
            }
            else
            {
                throw new Exception(column.DataType + " is not a supported data type.");
            }
        }      
    }
}
