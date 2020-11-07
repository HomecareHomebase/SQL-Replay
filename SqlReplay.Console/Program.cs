using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace SqlReplay.Console
{
    using System.Threading.Tasks;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Collections.Generic;
    using System;
    using CustomPreProcessing;
    using System.Linq;
    using Newtonsoft.Json;

    internal class Program
    {
        internal static async Task Main(string[] args)
        {
            string command = args[0];

            string inputDirectory;
            string outputDirectory;
            short clients;
            string filePath;
            string outputFilePath;
            int durationInMinutes;
            string storedProcedureNames;
            string cs;

            switch (command)
            {
                case "prep":
                    inputDirectory = args[1];
                    outputDirectory = args[2];
                    clients = short.Parse(args[3]);
                    cs = args[4];

                    await Prep(Directory.GetFiles(inputDirectory), outputDirectory, clients, cs);
                    break;
                case "prepnosc":
                    outputDirectory = args[1];
                    clients = short.Parse(args[2]);
                    int numSessions = int.Parse(args[3]);
                    DateTime start = DateTime.Parse(args[4]);
                    string spName = args[5];
                    string jsonParmPath = args[6];

                    int? runTimeHours = null;
                    if (args.Length > 7)
                    {
                        runTimeHours = (int?)int.Parse(args[7]);
                    }

                    await PrepNewOrSignatureChange(outputDirectory, clients, numSessions, start, spName, jsonParmPath, runTimeHours);
                    break;
                case "warmup":
                    filePath = args[1];
                    int.TryParse(args[2], out durationInMinutes);
                    storedProcedureNames = args[3];                    
                    cs = null;
                    if (args.Length > 4)
                    {
                        cs = args[4];
                    }

                    await Warmup(filePath, durationInMinutes, storedProcedureNames.Split(','), cs);
                    break;
                case "run":
                    filePath = args[1];
                    DateTimeOffset.TryParse(args[2], out var restorePoint);
                    int.TryParse(args[3], out durationInMinutes);
                    storedProcedureNames = args[4];
                    cs = null;
                    if (args.Length > 5)
                    {
                        cs = args[5];
                    }

                    await Run(filePath, restorePoint, durationInMinutes, storedProcedureNames.Split(','), cs);
                    break;
                case "output":
                    inputDirectory = args[1];
                    outputFilePath = args[2];
                    storedProcedureNames = args[3];
                    await Output(Directory.GetFiles(inputDirectory), outputFilePath, storedProcedureNames.Split(','));
                    break;
                case "convert":
                    filePath = args[1];
                    outputFilePath = args[2];
                    cs = args[3];
                    await Convert(filePath, outputFilePath, cs);
                    break;
            }
        }

        internal static async Task Prep(string[] filePaths, string outputDirectory, int clients, string connectionString)
        {
            var preProcessor = new PreProcessor();
            Run run = await preProcessor.PreProcessAsync(filePaths, connectionString);

            await ProcessPrep(run, outputDirectory, clients, connectionString);
        }

        internal static async Task PrepNewOrSignatureChange(string outputDirectory, int clients, int numberOfSessions, DateTime startDateTime, string storedProcedureName, string jsonParmPath, int? runTimeInHours)
        {
            var preProcessor = new CustomPreProcessor(numberOfSessions, startDateTime, storedProcedureName, jsonParmPath, runTimeInHours);
            Run run = preProcessor.GenerateRun();

            await ProcessPrep(run, outputDirectory, clients, null);
        }

        internal static async Task ProcessPrep(Run run, string outputDirectory, int clients, string connectionString)
        {
            Run[] runs = new Run[clients];
            for (var i = 0; i < clients; ++i)
            {
                runs[i] = new Run { Sessions = new List<Session>(), ConnectionString = connectionString, EventCaptureOrigin = run.EventCaptureOrigin };
            }

            for (var i = 0; i < run.Sessions.Count; ++i)
            {
                var bucket = i % clients;
                runs[bucket].Sessions.Add(run.Sessions[i]);
            }

            var formatter = new BinaryFormatter();
            for (var i = 0; i < clients; ++i)
            {
                using (var stream = new MemoryStream())
                {
                    formatter.Serialize(stream, runs[i]);
                    await File.WriteAllBytesAsync($@"{outputDirectory}\replay{i}.txt", stream.ToArray());
                }
            }
        }

        internal static async Task Warmup(string filePath, int durationInMinutes, string[] storedProcedureNamesToInclude, string connectionString = null)
        {
            Run run = await DeserializeRun(filePath);
            if (connectionString != null)
            {
                run.ConnectionString = connectionString;
            }

            var runner = new Runner();
            await runner.WarmupAsync(run, durationInMinutes, storedProcedureNamesToInclude);
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
            string logFilePath = Path.Combine(Path.GetDirectoryName(filePath), Path.GetFileName(filePath).Replace(fileNameWithoutExtension, fileNameWithoutExtension + "_log"));
            using (StreamWriter writer = new StreamWriter(logFilePath, false))
            {
                await writer.WriteLineAsync($"{runner.Exceptions.Count} exceptions captured");
                foreach (var ex in runner.Exceptions)
                {
                    await writer.WriteLineAsync(ex.ToString());
                }
            }
        }

        internal static async Task Run(string filePath, DateTimeOffset restorePoint, int durationInMinutes, string[] storedProcedureNamesToExclude, string connectionString = null)
        {
            Run run = await DeserializeRun(filePath);            
            if (connectionString != null)
            {
                run.ConnectionString = connectionString;
            }

            var runner = new Runner();
            await runner.RunAsync(run, restorePoint, durationInMinutes, storedProcedureNamesToExclude);
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
            string logFilePath = Path.Combine(Path.GetDirectoryName(filePath), Path.GetFileName(filePath).Replace(fileNameWithoutExtension, fileNameWithoutExtension + "_log"));
            using (StreamWriter writer = new StreamWriter(logFilePath, false))
            {
                await writer.WriteLineAsync($"{runner.Exceptions.Count} exceptions captured");
                foreach (var ex in runner.Exceptions)
                {
                    await writer.WriteLineAsync(ex.ToString());
                }
            }
        }

        internal static async Task Output(string[] filePaths, string outputFilePath, string[] storedProcedureNames)
        {
            var matchCriteria = StoredProcedureSearch.CreateMatchCriteria(storedProcedureNames);
            List<Event> events = new List<Event>();
            foreach (var filePath in filePaths)
            {                
                if (!Regex.IsMatch(Path.GetFileName(filePath), @"^replay\d+\.txt$", RegexOptions.IgnoreCase))
                {
                    //ignore any files that don't fit the pattern of replay files outputted from prep or prepnosc
                    continue;
                }
                Run run = await DeserializeRun(filePath);
                events.AddRange(run.Sessions.SelectMany(s => s.Events)
                    .Where(e => matchCriteria.Any(
                        mc => ((e as Rpc)?.Procedure?.Equals(mc, StringComparison.CurrentCultureIgnoreCase)).GetValueOrDefault())).ToArray());
                foreach (var evt in events)
                {
                    ((Rpc) evt).TimeElapsedInMilliseconds =
                        (int)evt.Timestamp.Subtract(run.EventCaptureOrigin).TotalMilliseconds;
                }
            }
            await File.WriteAllTextAsync(outputFilePath, JsonConvert.SerializeObject(events.OrderBy(e => e.EventSequence)));
        }

        internal static async Task Convert(string filePath, string outputFilePath, string connectionString)
        {
            List<Rpc> rpcs = new List<Rpc>();
            PreProcessor preProcessor = new PreProcessor();
            using (var con = new SqlConnection(connectionString))
            {
                await con.OpenAsync();
                using (var reader = new StreamReader(filePath))
                {
                    await reader.ReadLineAsync(); //skip header
                    while (!reader.EndOfStream)
                    {
                        string statement = await reader.ReadLineAsync();
                        Rpc rpc = new Rpc
                        {
                            Statement = statement.TrimStart('"').TrimEnd('"')
                        };
                        preProcessor.LoadParameters(con, rpc);
                        rpcs.Add(rpc);
                    }
                }
            }
            await File.WriteAllTextAsync(outputFilePath, JsonConvert.SerializeObject(rpcs));
        }

        private static async Task<Run> DeserializeRun(string filePath)
        {
            Run run;
            var formatter = new BinaryFormatter();
            using (var stream = new MemoryStream(await File.ReadAllBytesAsync(filePath)))
            {
                run = (Run)formatter.Deserialize(stream);
            }
            return run;
        }
    }
}