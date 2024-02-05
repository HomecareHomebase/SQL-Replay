using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using Microsoft.SqlServer.Server;

namespace SqlReplay.Console;

public class RpcInterpreter
{
    private readonly Rpc _rpc;
    private readonly string _commandText;
    private readonly CommandType _commandType;

    public RpcInterpreter(Rpc rpc)
    {
        _rpc = rpc;
        if (!string.IsNullOrWhiteSpace(rpc.Procedure))
        {
            _commandText = rpc.Procedure;
            _commandType = CommandType.StoredProcedure;
        }
        else
        {
            _commandText = rpc.Statement;
            _commandType = CommandType.Text;
        }
    }

    public SqlCommand GetSqlCommand(SqlConnection conn, int timeout)
    {
        var cmd =  new SqlCommand(_commandText, conn)
        {
            CommandType = _commandType,
            CommandTimeout = timeout
        };
        foreach (var param in _rpc.Parameters)
        {
            cmd.Parameters.Add(ParseParameter(param));
        }
        
        return cmd;
    }

    public string GetSqlCommandText()
    {
        string query = _commandText;
        foreach (var param in _rpc.Parameters)
        {
            SqlParameter sqlParam = ParseParameter(param);
            
            query += $" @{sqlParam.ParameterName} = {sqlParam.Value}";
        }

        return query;
    }
    
    private SqlParameter ParseParameter(Parameter param)
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
                sqlParam.Value = SqlTypifyValue(param.Value, param.SqlDbType);
            }

            return sqlParam;
        }
    
        /// <summary>
        /// Converts special case input into appropriate SQL values. See Remarks.
        /// </summary>
        /// <param name="inputValue">The unconverted parameter value to send to SQL</param>
        /// <param name="sqlDbType">The SQL Type of the parameter</param>
        /// <returns>A special default value if <paramref name="inputValue"/> is "" and <paramref name="sqlDbType"/> is
        /// SqlDbType.Int or DateTime; otherwise, <paramref name="inputValue"/></returns>
        /// <remarks>
        /// <para>This oddball little method is required because some of our executions pass '' (empty string) to sproc
        /// parameters of type INT or DATETIME. In SQL-Replay, though, ADO.Net is intercepting those executions
        /// and complaining that "" doesn't convert to an Int32.
        /// To limit risk exposure, this method is very tightly scoped to address only these two known occurring
        /// problems.</para>
        /// <para>Try this fun SQL out for a demo of what kind of type wrangling SQL allows when .NET isn't
        /// mediating:</para>
        /// <code>
        /// DECLARE @defaultInt INT, @emptyStringInt INT = '', @oneSpaceInt INT = ' ', @twoSpaceInt INT = '  '
        /// DECLARE @defaultdt DATETIME, @emptyStringDt DATETIME = '', @oneSpaceDt DATETIME = ' ', @twoSpaceDt DATETIME = '  '
        /// SELECT @defaultInt, @emptyStringInt, @oneSpaceInt, @twoSpaceInt
        /// SELECT @defaultdt, @emptyStringDt, @oneSpaceDt, @twoSpaceDt
        /// -- FYI conversion fails for all the following examples:
        /// DECLARE @tabInt INT = '	'
        /// DECLARE @tabDt DATETIME = '	'
        /// DECLARE @newlineInt INT = '
        /// '
        /// DECLARE @newlineDt DATETIME = '
        /// '
        /// DECLARE @alphaInt INT = 'abc'
        /// DECLARE @alphaDt DATETIME = 'abc'
        /// </code>
        /// </remarks>
        private static object SqlTypifyValue(object inputValue, SqlDbType sqlDbType)
        {
            return sqlDbType switch
            {
                SqlDbType.Int when inputValue is "" => 0,
                SqlDbType.DateTime when inputValue is "" => DateTime.Parse("1900-01-01 00:00:00.000",
                    CultureInfo.InvariantCulture),
                _ => inputValue
            };
        }
}
