namespace SqlReplay.Console
{
    using System;
    using System.Threading.Tasks;
    using System.Data.SqlClient;
    using System.Globalization;
    using System.Collections.Generic;
    using System.Linq;
    using System.Data;
    using Microsoft.SqlServer.Server;
    using System.Transactions;

    internal class EventExecutor
    {       
        public List<TimestampedException> Exceptions { get; set; } = new List<TimestampedException>();

        public async Task ExecuteSessionEventsAsync(DateTimeOffset eventCaptureOrigin, DateTimeOffset replayOrigin, IEnumerable<Session> sessions, string connectionString, IRunnerSettings runnerSettings)
        {
            List<Task> sessionTasks = new List<Task>();
            BucketStats stats = new BucketStats();
            foreach (var session in sessions)
            {
                sessionTasks.Add(Task.Run(async () =>
                {
                    List<Task> evtTasks = new List<Task>();
                    foreach (var evt in session.Events)
                    {
                        evtTasks.Add(Task.Run(async () =>
                        {
                            TimeSpan timeToDelay = evt.Timestamp.Subtract(eventCaptureOrigin).Subtract(DateTimeOffset.UtcNow.Subtract(replayOrigin));
                            if (timeToDelay.TotalMilliseconds > 0)
                            {
                                await Task.Delay(timeToDelay);
                            }
                            else
                            {
                                stats.timesFellBehind++;
                            }
                            
                            if (timeToDelay.TotalMilliseconds < stats.minDelayms)
                            {
                                stats.minDelayms = timeToDelay.TotalMilliseconds;
                            }
                            if (timeToDelay.TotalMilliseconds > stats.maxDelayms)
                            {
                                stats.maxDelayms = timeToDelay.TotalMilliseconds;
                            }

                            if (evt is Transaction transaction)
                            {
                                //Only pay attention to Begin as any Rollback at this level would not have a corresponding Begin
                                if (transaction.TransactionState == "Begin")
                                {
                                    try
                                    {
                                        using (var transactionScope = new TransactionScope(
                                            TransactionScopeOption.Required,
                                            new TransactionOptions()
                                            {
                                                IsolationLevel = System.Transactions.IsolationLevel.ReadUncommitted,
                                                Timeout = TimeSpan.FromSeconds(runnerSettings.TransactionScopeTimeout)
                                            },
                                            TransactionScopeAsyncFlowOption.Enabled))
                                        {
                                            await ExecuteTransactionEventsAsync(eventCaptureOrigin, replayOrigin, transaction, connectionString, runnerSettings);
                                            transactionScope.Complete();
                                        }
                                    }
                                    catch (TransactionAbortedException)
                                    {
                                        //These are simply rollbacks, no need to log
                                    }
                                    catch (Exception ex)
                                    {
                                        this.Exceptions.Add(new TimestampedException(ex));
                                    }
                                }
                            }
                            else if (evt is Rpc rpc)
                            {
                                var rpcInterpreter = new RpcInterpreter(rpc);
                                try
                                {
                                    await RetryDeadlock(async () =>
                                    {
                                        using (var sqlConnection = new SqlConnection(connectionString))
                                        {
                                            await sqlConnection.OpenAsync();
                                            using (var sqlCommand = rpcInterpreter.GetSqlCommand(sqlConnection, runnerSettings.SqlCommandTimeout))
                                            {
                                                await sqlCommand.ExecuteNonQueryAsync();
                                            }
                                        }
                                    });
                                }
                                catch (Exception ex)
                                {
                                    this.Exceptions.Add(new TimestampedException(ex, rpcInterpreter.GetSqlCommandText()));
                                }
                            }
                            else if (evt is BulkInsert bulkInsert)
                            {
                                if (bulkInsert.Rows.Count == 0) { return; }
                                var dataTable = new DataTable();
                                foreach (var column in bulkInsert.Columns)
                                {
                                    dataTable.Columns.Add(GetDataColumn(column));
                                }
                                for (var rowIndex = 0; rowIndex < bulkInsert.Rows.Count; rowIndex++)
                                {
                                    DataRow dataRow = dataTable.NewRow();
                                    for (var columIndex = 0; columIndex < bulkInsert.Columns.Count; columIndex++)
                                    {
                                        dataRow[columIndex] = bulkInsert.Rows[rowIndex][columIndex];
                                    }
                                    dataTable.Rows.Add(dataRow);
                                }
                                SqlBulkCopyOptions options;
                                if (bulkInsert.CheckConstraints && bulkInsert.FireTriggers)
                                {
                                    options = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers;
                                }
                                else if (bulkInsert.CheckConstraints)
                                {
                                    options = SqlBulkCopyOptions.CheckConstraints;
                                }
                                else if (bulkInsert.FireTriggers)
                                {
                                    options = SqlBulkCopyOptions.FireTriggers;
                                }
                                else
                                {
                                    options = SqlBulkCopyOptions.Default;
                                }
                                try
                                {
                                    await RetryDeadlock(async () =>
                                    {
                                        using (var sqlConnection = new SqlConnection(connectionString))
                                        {
                                            await sqlConnection.OpenAsync();
                                            using (var bulkCopy = new SqlBulkCopy(sqlConnection, options, null)
                                                {BulkCopyTimeout = runnerSettings.BulkCopyTimeout})
                                            {
                                                bulkCopy.DestinationTableName = bulkInsert.Table;
                                                foreach (DataColumn column in dataTable.Columns)
                                                {
                                                    bulkCopy.ColumnMappings.Add(
                                                        new SqlBulkCopyColumnMapping(column.ColumnName,
                                                            column.ColumnName));
                                                }

                                                await bulkCopy.WriteToServerAsync(dataTable);
                                            }
                                        }
                                    });
                                }
                                catch (Exception ex)
                                {
                                    this.Exceptions.Add(new TimestampedException(ex, bulkInsert.BatchText));
                                }
                            }
                        }));
                        await Task.WhenAll(evtTasks);
                    }
                }));
            }
            await Task.WhenAll(sessionTasks);
            Console.WriteLine(DateTime.Now + " - Ending bucket: " + sessions.First().Events.First().Timestamp);
            Console.WriteLine(DateTime.Now + $" - Bucket Stats - TimesFellBehind={stats.timesFellBehind}, MinDelay={stats.minDelayms}, MaxDelay={stats.maxDelayms}, SessionCount={sessions.Count()}, EventCount={sessions.Sum(s => s.Events.Count())}");
        }

        private async Task ExecuteTransactionEventsAsync(DateTimeOffset eventCaptureOrigin, DateTimeOffset replayOrigin, Transaction transaction, string connectionString, IRunnerSettings runnerSettings)
        {
            foreach (var evt in transaction.Events)
            {
                TimeSpan timeToDelay = evt.Timestamp.Subtract(eventCaptureOrigin).Subtract(DateTimeOffset.UtcNow.Subtract(replayOrigin));
                if (timeToDelay.TotalMilliseconds > 0)
                {
                    await Task.Delay(timeToDelay);
                }
                if (evt is Transaction nestedTransaction)
                {
                    if (nestedTransaction.TransactionState == "Begin")
                    {
                        using (var nestedTransactionScope = new TransactionScope(
                            TransactionScopeOption.Required,
                            new TransactionOptions()
                            {
                                IsolationLevel = System.Transactions.IsolationLevel.ReadUncommitted,
                                Timeout = TimeSpan.FromSeconds(runnerSettings.TransactionScopeTimeout)
                            },
                            TransactionScopeAsyncFlowOption.Enabled))
                        {
                            await ExecuteTransactionEventsAsync(eventCaptureOrigin, replayOrigin, nestedTransaction, connectionString, runnerSettings);
                            nestedTransactionScope.Complete();
                        }
                    }
                    else if (nestedTransaction.TransactionState == "Rollback")
                    {
                        throw new TransactionAbortedException();
                    }
                }
                else if (evt is Rpc rpc)
                {
                    var rpcInterpreter = new RpcInterpreter(rpc);
                    await RetryDeadlock(async () =>
                    {
                        using (var sqlConnection = new SqlConnection(connectionString))
                        {
                            await sqlConnection.OpenAsync();
                            using (var sqlCommand = rpcInterpreter.GetSqlCommand(sqlConnection, runnerSettings.SqlCommandTimeout))
                            {
                                await sqlCommand.ExecuteNonQueryAsync();
                            }
                        }
                    });
                }
                else if (evt is BulkInsert bulkInsert)
                {
                    if (bulkInsert.Rows.Count == 0) { continue; }
                    var dataTable = new DataTable();
                    foreach (var column in bulkInsert.Columns)
                    {
                        dataTable.Columns.Add(GetDataColumn(column));
                    }
                    for (var rowIndex = 0; rowIndex < bulkInsert.Rows.Count; rowIndex++)
                    {
                        DataRow dataRow = dataTable.NewRow();
                        for (var columIndex = 0; columIndex < bulkInsert.Columns.Count; columIndex++)
                        {
                            dataRow[columIndex] = bulkInsert.Rows[rowIndex][columIndex];
                        }
                        dataTable.Rows.Add(dataRow);
                    }
                    SqlBulkCopyOptions options;
                    if (bulkInsert.CheckConstraints && bulkInsert.FireTriggers)
                    {
                        options = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers;
                    }
                    else if (bulkInsert.CheckConstraints)
                    {
                        options = SqlBulkCopyOptions.CheckConstraints;
                    }
                    else if (bulkInsert.FireTriggers)
                    {
                        options = SqlBulkCopyOptions.FireTriggers;
                    }
                    else
                    {
                        options = SqlBulkCopyOptions.Default;
                    }
                    await RetryDeadlock(async () =>
                    {
                        using (var sqlConnection = new SqlConnection(connectionString))
                        {
                            await sqlConnection.OpenAsync();
                            using (var bulkCopy = new SqlBulkCopy(sqlConnection, options, null)
                                {BulkCopyTimeout = runnerSettings.BulkCopyTimeout})
                            {
                                bulkCopy.DestinationTableName = bulkInsert.Table;
                                foreach (DataColumn column in dataTable.Columns)
                                {
                                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(column.ColumnName,
                                        column.ColumnName));
                                }
                                await bulkCopy.WriteToServerAsync(dataTable);
                            }
                        }
                    });
                }
            }
        }

        private Task RetryDeadlock(Func<Task> func)
        {
            var tries = 0;
            while (tries < 3)
            {
                try
                {
                    return func();
                }
                catch (Exception ex)
                {
                    if (HasDeadlock(ex))
                    {
                        tries++;
                    }
                    else
                    {
                        throw;
                    }
                }
            }
            return Task.CompletedTask;
        }

        private bool HasDeadlock(Exception ex)
        {
            if (ex is SqlException sqlEx && sqlEx.Errors.Cast<SqlError>().Any(e =>
                e.Number == 1205 || (e.Number == 50000 && e.Message.Contains("deadlock"))))
            {
                return true;
            }
            return ex.InnerException != null && HasDeadlock(ex.InnerException);
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
            else if (column.SqlDbType.ToString().Contains("Binary") || column.SqlDbType.ToString().Contains("Image"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(byte[]) };
            }                    
            else if (column.SqlDbType.ToString().Contains("Text") || column.SqlDbType.ToString().Contains("Char"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(string) };
            }                    
            else if (column.SqlDbType.ToString() == "Bit")
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(bool) };
            }
            else if (column.SqlDbType.ToString().Contains("DateTimeOffset"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(DateTimeOffset) };
            }
            else if (column.SqlDbType.ToString().Contains("Date"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(DateTime) };
            }
            else if (column.SqlDbType.ToString().Contains("Time"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(TimeSpan) };
            }
            else if (column.SqlDbType.ToString().Contains("Decimal") || column.SqlDbType.ToString().Contains("Money"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(decimal) };
            }
            else if (column.SqlDbType.ToString().Contains("Float") || column.SqlDbType.ToString().Contains("Real"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(double) };
            }
            else if (column.SqlDbType.ToString().Contains("UniqueIdentifier"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(Guid) };
            }
            else if (column.SqlDbType.ToString().Contains("Variant"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(object) };
            }
            else if (column.SqlDbType.ToString().Contains("Xml"))
            {
                return new DataColumn { ColumnName = columnName, DataType = typeof(string) };
            }
            else
            {
                throw new Exception(column.SqlDbType + " is not a supported data type.");
            }
        }
    }
}
