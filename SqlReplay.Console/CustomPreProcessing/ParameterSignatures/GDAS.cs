using System;
using System.Data;

namespace SqlReplay.Console.CustomPreProcessing.ParameterSignatures
{
    public static class Gdas
    {
        /* gdGetBereavementRiskAssessmentHistory
                Name	         Type	    Length   Precision   Scale   IsOutput
                @agentid	     int	    4	     10	         0	     0
                @lastrenewtime   datetime   8	     23	         3	     0
        */

        public static Parameter AgentId => new Parameter
        {
            Name = "@agentid",
            DbType = SqlDbType.Int,
            Size = 4,
            Precision = (byte)10,
            Scale = (byte)0,
            Direction = ParameterDirection.Input,
            Value = DBNull.Value
        };

        public static Parameter LastRenewTime => new Parameter
        {
            Name = "@lastrenewtime",
            DbType = SqlDbType.DateTime,
            Size = 8,
            Precision = (byte)23,
            Scale = (byte)3,
            Direction = ParameterDirection.Input,
            Value = DBNull.Value
        };
    }
}
