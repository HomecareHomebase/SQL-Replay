namespace SqlReplay.Console
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Linq;
    using System.Data;

    public class Runner
    {
        public List<Exception> Exceptions { get; set; } = new List<Exception>();

        public Task Warmup(Run run, int durationInMinutes, string[] storedProcedureNamesToInclude)
        {
            var matchCriteria = StoredProcedureSearch.CreateMatchCriteria(storedProcedureNamesToInclude);

            List<Event> allEvents;
            if (matchCriteria.Any())                
            {
                if (durationInMinutes > 0)
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .Where(e => matchCriteria.Any(mc =>
                                        ((e as Rpc)?.Procedure?.Equals(mc, StringComparison.CurrentCultureIgnoreCase))
                                        .GetValueOrDefault()) &&
                                    e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes))
                        .ToList();
                }
                else
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .Where(e => matchCriteria.Any(mc =>
                                        ((e as Rpc)?.Procedure?.Equals(mc, StringComparison.CurrentCultureIgnoreCase))
                                        .GetValueOrDefault()))
                        .ToList();
                }
            }
            else
            {
                return Task.CompletedTask;
            }          
            return RunEvents(allEvents, run.EventCaptureOrigin, run.ConnectionString);
        }

        public Task Run(Run run, int durationInMinutes, string[] storedProcedureNamesToExclude)
        {           
            var matchCriteria = StoredProcedureSearch.CreateMatchCriteria(storedProcedureNamesToExclude);

            List<Event> allEvents;
            if (matchCriteria.Any())
            {
                if (durationInMinutes > 0)
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .Where(e => !matchCriteria.Any(mc =>
                            ((e as Rpc)?.Procedure?.Equals(mc, StringComparison.CurrentCultureIgnoreCase))
                            .GetValueOrDefault()) &&
                                    e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes))
                        .ToList();
                }
                else
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .Where(e => !matchCriteria.Any(mc =>
                            ((e as Rpc)?.Procedure?.Equals(mc, StringComparison.CurrentCultureIgnoreCase))
                            .GetValueOrDefault()))
                        .ToList();
                }
            }
            else
            {
                if (durationInMinutes > 0)
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .Where(e => e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes))
                        .ToList();
                }
                else
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .ToList();
                }                
            }
            return RunEvents(allEvents, run.EventCaptureOrigin, run.ConnectionString);
        }

        public async Task RunEvents(List<Event> allEvents, DateTimeOffset eventCaptureOrigin, string connectionString)
        {            
            //Prepare threads in thread pool
            System.Threading.ThreadPool.SetMaxThreads(32767, 32767);
            System.Threading.ThreadPool.SetMinThreads(32767, 32767);

            Console.WriteLine("Preparing 15 second buckets...");

            var allDependentEvents = allEvents.Where(e => e.TransactionId != "0" && ((e is Rpc) || (e as Transaction)?.TransactionState != "Begin")).ToList();
            var allIndependentEvents = allEvents.Where(e => e.TransactionId == "0" || (e as Transaction)?.TransactionState == "Begin");

            var groups = allIndependentEvents.GroupBy(r => r.Timestamp.ToString("ddhhmm") + r.Timestamp.Second / 15).OrderBy(g => g.Key);
            var buckets = new Dictionary<string, List<Event>>();
            foreach (var group in groups)
            {
                var trxStarts = group.Where(e => e.TransactionId != "0").Select(e => (e as Transaction)).ToList();
                var startHash = new HashSet<string>(trxStarts.Select(x => x.TransactionId));
                var startDict = new Dictionary<string, Transaction>();
                foreach (var trx in trxStarts)
                {
                    trx.Events = new List<Event>();
                    startDict.Add(trx.TransactionId, trx);
                }

                var bucket = new List<Event>(group);

                foreach(var evt in allDependentEvents)
                {
                    if (startHash.Contains(evt.TransactionId))
                    {
                        startDict[evt.TransactionId].Events.Add(evt);                        
                    }                    
                }
                
                foreach (var trx in trxStarts)
                {
                    trx.Events = trx.Events.OrderBy(e => e.EventSequence).ToList();
                    //Avoid stragglers at end of trace that don't include a commit or rollback
                    if (!trx.Events.Any(e => (e as Transaction)?.TransactionState == "Commit" ||
                                             (e as Transaction)?.TransactionState == "Rollback"))
                    {
                        Event model;
                        if (trx.Events.Count > 0)
                        {
                            model = trx.Events.Last();
                        }
                        else
                        {
                            model = trx;
                        }
                        trx.Events.Add(new Transaction
                        {
                            TransactionId = model.TransactionId,
                            TransactionState = "Commit",
                            EventSequence = model.EventSequence + 1,
                            Timestamp = model.Timestamp.AddMilliseconds(1)
                        });
                    }
                }

                buckets.Add(group.Key, bucket);              
            }

            var bucketDependentEvents = buckets.SelectMany(b => b.Value.Where(i => i is Transaction).SelectMany(i => (i as Transaction)?.Events));
            var orphanEvents = allDependentEvents.Except(bucketDependentEvents);
            foreach (var evt in orphanEvents)
            {
                string key = evt.Timestamp.ToString("ddhhmm") + evt.Timestamp.Second / 15;
                if (buckets.TryGetValue(key, out var events))
                {
                    events.Add(evt);
                }
                else
                {
                    buckets.Add(key, new List<Event> { evt });
                }
            }

            var orderedBuckets = buckets.OrderBy(b => b.Key).Select(b => b.Value.OrderBy(e => e.Timestamp).ToList()).ToList();

            Console.WriteLine("Kicking off executions...");

            List<Task> tasks = new List<Task>();
            var eventExecutor = new EventExecutor();
            DateTimeOffset replayOrigin = DateTimeOffset.UtcNow;

            foreach (var bucket in orderedBuckets)
            {
                Console.WriteLine("Starting bucket: " + bucket[0].Timestamp);
                tasks.Add(eventExecutor.ExecuteEvents(eventCaptureOrigin, replayOrigin, bucket, connectionString));
                await Task.Delay(15000);
                Console.WriteLine("Ending Delay: " + bucket[0].Timestamp);
            }

            Console.WriteLine("Waiting for unfinished executions to complete...");
            await Task.WhenAll(tasks);
            Console.WriteLine("Executions complete.");
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
