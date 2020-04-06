namespace SqlReplay.Console
{
    using System.Threading.Tasks;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Collections.Generic;
    using System;

    internal class Program
    {
        internal static async Task Main(string[] args)
        {
            string command = args[0];

            if (command == "prep")
            {
                string inputPath = args[1];
                string outputPath = args[2];
                short clients = short.Parse(args[3]);
                string connectionString = args[4];
                DateTimeOffset? cutoff = null;
                if (args.Length > 5)
                {
                    cutoff = DateTimeOffset.Parse(args[5]);
                }

                await Prep(Directory.GetFiles(inputPath), outputPath, clients, connectionString, cutoff);
            }
            else if (command == "run")
            {
                string filePath = args[1];
                string cs = null;
                if (args.Length > 2)
                {
                    cs = args[2];
                }

                await Run(filePath, cs);
            }
        }

        internal static async Task Prep(string[] fileNames, string outputPath, int clients, string connectionString, DateTimeOffset? cutoff)
        {
            var preProcessor = new PreProcessor();
            Run run = await preProcessor.PreProcess(fileNames, connectionString, cutoff);
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
                    await File.WriteAllBytesAsync($@"{outputPath}\replay{i}.txt", stream.ToArray());
                }
            }
        }

        internal static async Task Run(string filePath, string connectionString = null)
        {
            Run run;
            var formatter = new BinaryFormatter();
            using (var stream = new MemoryStream(await File.ReadAllBytesAsync(filePath)))
            {
                run = (Run)formatter.Deserialize(stream);
            }
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
    }
}
