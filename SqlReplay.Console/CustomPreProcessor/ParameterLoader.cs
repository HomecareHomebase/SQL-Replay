using System.Linq;
using System.Collections.Generic;
using System.Data;
using System;

namespace SqlReplay.Console.CustomPreProcessing
{
    internal class ParameterLoader
    {
        private readonly IList<JsonParameterInfo> jsonParameterInfo;
        private int currentIterativeIndex = 0;

        internal ParameterLoader(IList<JsonParameterInfo> jsonParameterInfo)
        {
            this.jsonParameterInfo = jsonParameterInfo;
        }

        internal void LoadParameters(Rpc rpc)
        {
            foreach(JsonParameterInfo infoItem in jsonParameterInfo)
            {
                rpc.Parameters.Add(GetParameter(infoItem));
            }
        }

        private Parameter GetParameter(JsonParameterInfo parameterInfo)
        {
            Parameter p = new Parameter
            {
                Name = parameterInfo.parameterName,
                DbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), parameterInfo.parameterProperties["DBType"].ToString(), true),
                Size = int.Parse(parameterInfo.parameterProperties["Size"].ToString()),
                Precision = byte.Parse(parameterInfo.parameterProperties["Precision"].ToString()),
                Scale = byte.Parse(parameterInfo.parameterProperties["Scale"].ToString()),
                Direction = (ParameterDirection)Enum.Parse(typeof(ParameterDirection), parameterInfo.parameterProperties["Direction"].ToString(), true),
                Value = GetParameterValue(parameterInfo.assignmentMethod, parameterInfo.parameterValues, parameterInfo.parameterName, (bool)parameterInfo.parameterProperties["IsNullable"])
            };

            return p;
        }

        private object GetParameterValue(ParameterAssignmentMethod assignmentMethod, IList<object> parameterValues, string parameterName, bool isNullable)
        {
            object returnValue = null;

            if (parameterValues.Count > 0)
            {
                switch (assignmentMethod)
                {
                    case ParameterAssignmentMethod.Iterative:
                        returnValue = GetIterativeParameterValue(parameterValues);
                        break;
                    case ParameterAssignmentMethod.Static:
                        returnValue = GetStaticParameterValue(parameterValues); // 
                        break;
                    case ParameterAssignmentMethod.Random:
                        returnValue = GetRandomParameterValue(parameterValues);
                        break;
                    default:
                        throw new ArgumentException("No or unknown parameter assignment type provided. Please review your json parameter information file.");
                }
            }

            if(returnValue == null && !isNullable)
            {
                throw new ArgumentNullException(parameterName, "Attempting to assign null value to non-nullable parameter.");
            }

            return returnValue ?? DBNull.Value;
        }

        private object GetIterativeParameterValue(IList<object> parameterValues)
        {
            object value = parameterValues.ElementAt(currentIterativeIndex);
            currentIterativeIndex = currentIterativeIndex + 1 >= parameterValues.Count() ? 0 : currentIterativeIndex + 1;

            return value;
        }

        private object GetStaticParameterValue(IList<object> parameterValues)
        {
            return parameterValues.ElementAt(0);
        }

        private object GetRandomParameterValue(IList<object> parameterValues)
        {
            Random rand = new Random();

            return parameterValues.ElementAt(rand.Next(parameterValues.Count));
        }
    }
}