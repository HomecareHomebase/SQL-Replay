using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Newtonsoft.Json;

namespace SqlReplay.Batch
{
    class Program
    {
        const int DefaultStatusCheckIntervalInSec = 60;

        const int DefaultReplayDurationInMinutes = 0; // replay all

        static void Main(string[] args)
        {
            if (args == null || string.IsNullOrWhiteSpace(args[0]))
            {
                ShowUsage();
                Environment.Exit(-1);
            }

            string logPath = string.Empty;
            if (args.Length > 1 && !string.IsNullOrWhiteSpace(args[1]) && Directory.Exists(args[1]))
                logPath = Path.Combine(args[1], $"SqlReplay.Batch_{GetTimestamp()}.log");

            WriteMessage(logPath, $"{GetTimestamp()} - Validating configuration...");
            BatchConfig config = ValidateAndGetConfig(args[0], out List<string> validationResults);
            if (config == null)
            {
                WriteMessage(logPath, $"{GetTimestamp()} - Validation failed:");
                validationResults.ForEach(msg=>WriteMessage(logPath, msg));
                Environment.Exit(-1);
            }
            WriteMessage(logPath, $"{GetTimestamp()} - Validation completed");

            string appFileName = Path.GetFileName(config.ReplayConsolePath);
            string appPath = Path.GetDirectoryName(config.ReplayConsolePath);
            bool isDll = Path.GetExtension(config.ReplayConsolePath).ToUpper() == ".DLL";

            WriteMessage(logPath, $"{GetTimestamp()} - Processing started");

            foreach (Batch batch in config.Batches)
            {
                var processes = new List<Process>();
                WriteMessage(logPath, $"{Environment.NewLine}{GetTimestamp()} - Running batch {batch.ProcessingOrder} of {config.Batches.Length}. Batch ID: {batch.BatchId}");
                for (int i = 0; i < batch.NumOfInstances; i++)
                {
                    string inputFile = batch.InputFiles[i];
                    string arguments =
                        $"run {inputFile} \"{batch.StartAt}\" {batch.DurationInMinutes} \"\" \"{config.ConnectionString}\"";

                    if (isDll)
                        arguments = $"{appFileName} {arguments}";

                    WriteMessage(logPath, $"{arguments}");

                    processes.Add(new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            FileName = isDll ? "dotnet" : appFileName,
                            Arguments = arguments,
                            UseShellExecute = true,
                            RedirectStandardOutput = false,
                            RedirectStandardError = false,
                            CreateNoWindow = false,
                            WorkingDirectory = appPath
                        }
                    });
                }

                processes.ForEach(p => p.Start());
                int running = processes.Count;
                WriteMessage(logPath, $"{GetTimestamp()} - Started {running} replay instances");

                do
                {
                    Thread.Sleep(config.StatusCheckIntervalInSec * 1000);
                    processes.ForEach(p =>
                    {
                        if (p.HasExited)
                            running--;
                    });
                } while (running > 0);

                WriteMessage(logPath, $"{GetTimestamp()} - Finished batch {batch.ProcessingOrder} of {config.Batches.Length}. Batch ID: {batch.BatchId}");
            }

            WriteMessage(logPath, $"{Environment.NewLine}{GetTimestamp()} - Processing completed");
            Environment.Exit(0);
        }

        static BatchConfig ValidateAndGetConfig(string batchConfigPath, out List<string> validationResults)
        {
            if(string.IsNullOrWhiteSpace(batchConfigPath))
                throw new ArgumentNullException(nameof(batchConfigPath));

            validationResults = new List<string>();

            if (!File.Exists(batchConfigPath))
            {
                validationResults.Add($"Batch configuration file {batchConfigPath} not found");
                return null;
            }

            string configJson = File.ReadAllText(batchConfigPath);
            BatchConfig config;
            try
            {
                config = JsonConvert.DeserializeObject<BatchConfig>(configJson);
            }
            catch (Exception e)
            {
                validationResults.Add($"Error deserializing {batchConfigPath}");
                validationResults.Add(e.ToString());
                return null;
            }

            if (config.Batches == null || config.Batches.Length < 1)
                validationResults.Add("No batch definitions found");

            if (string.IsNullOrWhiteSpace(config.ConnectionString))
                validationResults.Add("connectionString is required");

            if (string.IsNullOrWhiteSpace(config.OutputPath))
                validationResults.Add("outputPath is required");

            if (string.IsNullOrWhiteSpace(config.ReplayConsolePath))
                validationResults.Add("replayConsolePath is required");

            if (!File.Exists(config.ReplayConsolePath))
                validationResults.Add($"Replay console {config.ReplayConsolePath} not found");

            if (!Directory.Exists(config.OutputPath))
                validationResults.Add($"Replay output path {config.OutputPath} not found");

            if (validationResults.Count > 0)
                return null;

            if (config.StatusCheckIntervalInSec <= 0)
                config.StatusCheckIntervalInSec = DefaultStatusCheckIntervalInSec;

            // ReSharper disable once AssignNullToNotNullAttribute
            config.Batches = config.Batches.OrderBy(b => b.ProcessingOrder).ToArray();

            var orderDuplicates = config.Batches.GroupBy(x => x.ProcessingOrder)
                .Where(y => y.Count() > 1)
                .Select(z => z.Key)
                .ToArray();

            if (orderDuplicates.Any())
                validationResults.Add("processingOrder duplicates found in batch definitions");

            var batchIdDuplicates = config.Batches.GroupBy(x => x.BatchId)
                .Where(y => y.Count() > 1)
                .Select(z => z.Key)
                .ToArray();

            if (batchIdDuplicates.Any())
                validationResults.Add("batchId duplicates found in batch definitions");

            if (validationResults.Count > 0)
                return null;

            int i = 0;
            Regex regex = new Regex(@"replay[\d]+.txt");

            // ReSharper disable once PossibleNullReferenceException
            foreach (Batch batch in config.Batches)
            {
                int numOfInstances = batch.NumOfInstances;
                string batchId = batch.BatchId;
                string startAt = batch.StartAt;
                int processingOrder = batch.ProcessingOrder;
                i++;

                if (numOfInstances < 1 || processingOrder < 1 || string.IsNullOrWhiteSpace(batchId) || string.IsNullOrWhiteSpace(startAt))
                {
                    validationResults.Add($"Batch #{i}: incomplete or invalid batch definition");
                    continue;
                }

                string batchPath = Path.Combine(config.OutputPath, batchId);
                if (!Directory.Exists(batchPath))
                {
                    validationResults.Add($"Batch #{i}: directory {batchPath} not found");
                    continue;
                }

                string[] files = Directory.EnumerateFiles(batchPath, "replay*.txt").ToArray();
                if (!files.Any())
                {
                    validationResults.Add($"Batch #{i}: Directory {batchPath} is empty");
                    continue;
                }

                var inputFiles = files.Where(file => regex.IsMatch(file)).ToArray();
                if (inputFiles.Length != batch.NumOfInstances)
                {
                    validationResults.Add($"Batch #{i}: Number of replay*.txt input files should match numOfInstances defined for the batch");
                    continue;
                }

                batch.InputFiles = inputFiles;
                if (batch.DurationInMinutes < 0)
                    batch.DurationInMinutes = DefaultReplayDurationInMinutes;
            }
            return validationResults.Count > 0 ? null : config;
        }

        static string GetTimestamp()
        {
            return string.Format($"{DateTime.UtcNow:MM-dd-yyyy--HH-mm-ss}");
        }

        static void WriteMessage(string logFilePath, string msg)
        {
            if (string.IsNullOrWhiteSpace(msg))
                throw new ArgumentNullException(nameof(msg));

            Console.WriteLine(msg);

            if (!string.IsNullOrWhiteSpace(logFilePath))
                File.AppendAllText(logFilePath, $"{msg}{Environment.NewLine}");
        }

        static void ShowUsage()
        {
            Console.WriteLine("Batch configuration file is required.");
            Console.WriteLine("Usage: SqlReplay.Batch.exe <batch config file path> [log directory path]");
            Console.WriteLine(@"Example: SqlReplay.Batch.exe C:\replay\config\SampleBatchConfig.json");
            Console.WriteLine(@"Example: SqlReplay.Batch.exe C:\replay\config\SampleBatchConfig.json C:\replay\output");
        }
    }
}
