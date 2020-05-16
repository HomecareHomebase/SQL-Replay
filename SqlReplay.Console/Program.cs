using System.Text.RegularExpressions;

namespace SqlReplay.Console
{
    using System.Threading.Tasks;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Collections.Generic;
    using System;
    using SqlReplay.Console.CustomPreProcessing;
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

            switch (command)
            {
                case "prep":
                    inputDirectory = args[1];
                    outputDirectory = args[2];
                    clients = short.Parse(args[3]);
                    string connectionString = args[4];

                    DateTimeOffset? cutoff = null;
                    if (args.Length > 5)
                    {
                        cutoff = DateTimeOffset.Parse(args[5]);
                    }

                    await Prep(Directory.GetFiles(inputDirectory), outputDirectory, clients, connectionString, cutoff);
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

                case "run":
                    string filePath = args[1];
                    string cs = null;
                    if (args.Length > 2)
                    {
                        cs = args[2];
                    }

                    await Run(filePath, cs);
                    break;
                case "output":
                    inputDirectory = args[1];
                    string outputFilePath = args[2];
                    string storedProcedureNames = args[3];
                    await Output(Directory.GetFiles(inputDirectory), outputFilePath, storedProcedureNames.Split(','));
                    break;
                default:
                    break;
            }
        }

        internal static async Task Prep(string[] filePaths, string outputDirectory, int clients, string connectionString, DateTimeOffset? cutoff)
        {
            var preProcessor = new PreProcessor();
            Run run = await preProcessor.PreProcess(filePaths, connectionString, cutoff);

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

        internal static async Task Run(string filePath, string connectionString = null)
        {
            Run run = await DeserializeRun(filePath);            
            if (connectionString != null)
            {
                run.ConnectionString = connectionString;
            }

            var runner = new Runner();
            await runner.Run(run);
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
            string logFilePath = Path.Combine(Path.GetDirectoryName(filePath), Path.GetFileName(filePath).Replace(fileNameWithoutExtension, fileNameWithoutExtension + "_log"));
            using (StreamWriter writer = new StreamWriter(logFilePath, false))
            {
                await writer.WriteLineAsync($"{runner.Exceptions.Count} exceptions captured");
                foreach (Exception ex in runner.Exceptions)
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
            }
            await File.WriteAllTextAsync(outputFilePath, JsonConvert.SerializeObject(events.OrderBy(e => e.EventSequence)));
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