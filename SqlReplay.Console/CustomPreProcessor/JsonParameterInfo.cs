using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace SqlReplay.Console.CustomPreProcessing
{
    [JsonObject]
    internal class JsonParameterInfo
    {
        [JsonProperty("ParameterName")]
        internal string parameterName { get; set; }

        [JsonProperty("AssignmentType")]
        internal ParameterAssignmentMethod assignmentMethod { get; set; }

        [JsonProperty("Properties")]
        internal Dictionary<string, object> parameterProperties { get; set; }

        [JsonProperty("Values")]
        internal List<Object> parameterValues { get; set; }

    } 

}
