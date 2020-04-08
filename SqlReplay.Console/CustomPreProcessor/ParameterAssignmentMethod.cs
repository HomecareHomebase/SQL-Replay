using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace SqlReplay.Console.CustomPreProcessing {
    [System.Flags]
    [JsonConverter(typeof(StringEnumConverter))]
    internal enum ParameterAssignmentMethod
    {
        Static = 1,
        Iterative = 2,
        Random = 3
    }
}