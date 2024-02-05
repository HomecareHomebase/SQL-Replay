namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Linq;
    using System.Data;
    using Microsoft.Extensions.Configuration;
    using System.Diagnostics;

    public class Runner
    {
        public List<TimestampedException> Exceptions { get; set; } = new List<TimestampedException>();

        public Task WarmupAsync(Run run, int durationInMinutes, string[] storedProcedureNamesToInclude)
        {
            var matchCriteria = StoredProcedureSearch.CreateMatchCriteria(storedProcedureNamesToInclude);
            if (matchCriteria.Any())                
            {
                if (durationInMinutes > 0)
                {
                    foreach (var session in run.Sessions)
                    {
                        session.Events.RemoveAll(e => !(matchCriteria.Any(mc =>
                                                          ((e as Rpc)?.Procedure?.Equals(mc, 
                                                              StringComparison.CurrentCultureIgnoreCase)).GetValueOrDefault()) &&
                                                      e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes)));
                    }
                }
                else
                {
                    foreach (var session in run.Sessions)
                    {
                        session.Events.RemoveAll(e => !matchCriteria.Any(mc =>
                                                            ((e as Rpc)?.Procedure?.Equals(mc,
                                                                StringComparison.CurrentCultureIgnoreCase)).GetValueOrDefault()));
                    }
                }
            }
            else
            {
                return Task.CompletedTask;
            }          
            return RunEventsAsync(run);
        }

        public Task RunAsync(Run run, DateTimeOffset restorePoint, int durationInMinutes, string[] storedProcedureNamesToExclude)
        {           
            var matchCriteria = StoredProcedureSearch.CreateMatchCriteria(storedProcedureNamesToExclude);
            if (matchCriteria.Any())
            {
                if (durationInMinutes > 0)
                {
                    foreach (var session in run.Sessions)
                    {
                        session.Events.RemoveAll(e => !(!EventMatchesCriteria(matchCriteria, e) &&
                                                      e.Timestamp > restorePoint &&
                                                      e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes)));
                    }
                }
                else
                {
                    foreach (var session in run.Sessions)
                    {
                        session.Events.RemoveAll(e => !(!EventMatchesCriteria(matchCriteria, e) &&
                                                        e.Timestamp > restorePoint));
                    }
                }
            }
            else
            {
                if (durationInMinutes > 0)
                {
                    foreach (var session in run.Sessions)
                    {
                        session.Events.RemoveAll(e => !(e.Timestamp > restorePoint &&
                                                        e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes)));
                    }
                }
                else
                {
                    foreach (var session in run.Sessions)
                    {
                        session.Events.RemoveAll(e => !(e.Timestamp > restorePoint));
                    }
                }                
            }
            foreach (var session in run.Sessions)
            {                
                session.Events.RemoveAll(e => 
                {
                    if (e is Rpc rpc && rpc.Statement != null)
                    {
                        return rpc.Statement.Contains("OPENROWSET", StringComparison.CurrentCultureIgnoreCase) ||
                                rpc.Statement.Contains("OPENDATASOURCE", StringComparison.CurrentCultureIgnoreCase);
                    }

                    return false;
                });
            }
            run.EventCaptureOrigin = restorePoint;
            return RunEventsAsync(run);
        }

        private static bool EventMatchesCriteria(List<string> matchCriteria, Event e)
        {
            string eventProcedure = (e as Rpc)?.Procedure;
            if (!string.IsNullOrEmpty(eventProcedure))
            {
                // the Procedure will be just the name of the sproc
                return matchCriteria.Any(mc =>
                    string.Equals(eventProcedure, mc, StringComparison.CurrentCultureIgnoreCase)
                );
            }
            string eventStatement = (e as Rpc)?.Statement;
            if (!string.IsNullOrEmpty(eventStatement))
            {
                // Statement could have text before or after the sproc name. This isn't an exact solution because the
                // sproc could be mentioned in non-functional code like a comment... which would be weird to include in a RPC statement.
                return matchCriteria.Any(mc =>
                    eventStatement.Contains(mc, StringComparison.CurrentCultureIgnoreCase));
            }
            return false;
        }

        private async Task RunEventsAsync(Run run)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .Build();

            IRunnerSettings runnerSettings = config.GetSection(nameof(RunnerSettings)).Get<RunnerSettings>();
          
            Console.WriteLine($"{DateTime.Now} - Warming up thread pool...");

            System.Threading.ThreadPool.SetMaxThreads(32767, 32767);
            System.Threading.ThreadPool.SetMinThreads(32767, 32767);

            Console.WriteLine($"{DateTime.Now} - Nesting events...");

            //Remove any sessions with no events
            run.Sessions.RemoveAll(s => s.Events.Count == 0);
            foreach (Session session in run.Sessions)
            {
                var nestedEvents = new List<Event>();
                Transaction parentTransaction = null;
                foreach (Event evt in session.Events)
                {
                    if (parentTransaction != null && evt.TransactionId != "0")
                    {
                        parentTransaction.Events.Add(evt);
                    }
                    else
                    {
                        nestedEvents.Add(evt);
                    }
                    if (evt is Transaction transaction)
                    {
                        switch (transaction.TransactionState)
                        {
                            case "Begin":
                                parentTransaction = transaction;
                                break;
                            case "Commit":
                            case "Rollback":
                                parentTransaction = session.Events
                                    .Where(e => ((e as Transaction)?.Events.Contains(parentTransaction)).GetValueOrDefault())
                                        .SingleOrDefault() as Transaction;
                                break;
                        }
                    }
                }
                session.Events = nestedEvents;
            }

            Console.WriteLine($"{DateTime.Now} - Preparing {runnerSettings.BucketInterval} second buckets of sessions...");
            
            var buckets = run.Sessions.GroupBy(s => 
            {
                Event firstEvt = s.Events.First();
                return firstEvt.Timestamp.ToString("ddHHmm") + firstEvt.Timestamp.Second / runnerSettings.BucketInterval;         
            })
            .OrderBy(g => g.Key)
            .Select(g => g.OrderBy(s => s.Events.First().Timestamp)
                .ToList()
            ).ToList();

            // Delay start time to sync across processes
            var pStartTime = Process.GetCurrentProcess().StartTime; // local time
            TimeSpan delay = pStartTime.AddMinutes(runnerSettings.StartDelayMinutes) - DateTime.Now;
            try
            {
                await Task.Delay(delay);
            }
            catch (ArgumentOutOfRangeException)
            {
                Console.WriteLine($"{DateTime.Now} - Syncing delay failed due to negative delay TimeSpan of {delay.TotalMilliseconds} ms. Process Start time: {pStartTime}");
            }


            Console.WriteLine($"{DateTime.Now} - Kicking off executions...");

            var tasks = new List<Task>();
            var eventExecutor = new EventExecutor();
            var replayOrigin = DateTimeOffset.UtcNow;

            foreach (var bucket in buckets)
            {
                var bucketTimestamp = bucket.First().Events.First().Timestamp;
                TimeSpan timeToDelay = bucketTimestamp.Subtract(run.EventCaptureOrigin).Subtract(DateTimeOffset.UtcNow.Subtract(replayOrigin));
                if (timeToDelay.TotalMilliseconds > 0)
                {
                    await Task.Delay(timeToDelay);
                }
                Console.WriteLine($"{DateTime.Now} - Starting bucket: {bucketTimestamp} - timeToDelay = {timeToDelay}");
                tasks.Add(eventExecutor.ExecuteSessionEventsAsync(run.EventCaptureOrigin, replayOrigin, bucket, run.ConnectionString, runnerSettings));
                Console.WriteLine($"{DateTime.Now} - Finished submitting bucket: {bucketTimestamp}");
            }

            Console.WriteLine($"{DateTime.Now} - Waiting for unfinished executions to complete...");
            await Task.WhenAll(tasks);
            Console.WriteLine($"{DateTime.Now} - Executions complete.");
            this.Exceptions.AddRange(eventExecutor.Exceptions);
        }       

        private void MakeRpcCallsWithTvpParametersSqlBatchCalls(IEnumerable<Event> events)
        {
            //Remove procedure name and parameters so proc calls with TVP variables will get executed as SQLBatch instead of RCP and get plan stored in cache
            foreach (var evt in events)
            {
                if (!(evt is Rpc rpc)) continue;
                if (rpc.Parameters.Any(p => p.SqlDbType == SqlDbType.Structured))
                {
                    rpc.Procedure = null;
                    rpc.Parameters.Clear();
                }
            }
        }

        private void DuplicateEvents(Run run, byte factor)
        {
            var storedProcedureNames = new string[]
            {
                "procedure I want to duplicate, including schema"
            };
            var matchCriteria = StoredProcedureSearch.CreateMatchCriteria(storedProcedureNames);            
            foreach (var session in run.Sessions)
            {
                var duplicates = new List<Rpc>();
                foreach (var evt in session.Events)
                {
                    if (!(evt is Rpc rpc)) continue;
                    if (!matchCriteria.Any(mc => (rpc.Procedure?.Equals(mc, StringComparison.CurrentCultureIgnoreCase)).GetValueOrDefault())) continue;
                    for (byte i = 0; i < factor - 1; i++)
                    {
                        var duplicate = new Rpc
                        {
                            EventSequence = rpc.EventSequence,
                            Statement = rpc.Statement,
                            ObjectName = rpc.ObjectName,
                            Procedure = rpc.Procedure,
                            Parameters = rpc.Parameters, //safe to share reference as this list will only be read from
                            Timestamp = rpc.Timestamp.AddMilliseconds(500 * (i + 1)),
                            TransactionId = "0" //don't share transaction ID to avoid duplicates and original from conflicting in one transaction
                        };
                        duplicates.Add(duplicate);
                    }
                }
                session.Events.AddRange(duplicates);
            }
        }
    }
}
