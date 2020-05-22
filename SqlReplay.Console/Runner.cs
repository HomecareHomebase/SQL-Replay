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

        public Task Run(Run run, DateTimeOffset restorePoint, int durationInMinutes, string[] storedProcedureNamesToExclude)
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
                                    e.Timestamp > restorePoint &&
                                    e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes))
                        .ToList();
                }
                else
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .Where(e => !matchCriteria.Any(mc =>
                            ((e as Rpc)?.Procedure?.Equals(mc, StringComparison.CurrentCultureIgnoreCase))
                            .GetValueOrDefault()) &&
                            e.Timestamp > restorePoint)
                        .ToList();
                }
            }
            else
            {
                if (durationInMinutes > 0)
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events)
                        .Where(e => e.Timestamp > restorePoint && e.Timestamp < run.EventCaptureOrigin.AddMinutes(durationInMinutes))
                        .ToList();
                }
                else
                {
                    allEvents = run.Sessions.SelectMany(s => s.Events).Where(e => e.Timestamp > restorePoint)
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

            //Dependent events are associated to a transaction but aren't the begin transaction
            var allDependentEvents = allEvents.Where(e => e.TransactionId != "0" && ((e is Rpc) || (e as Transaction)?.TransactionState != "Begin")).ToList();
            //Independent events have no transaction or are a begin transaction
            var allIndependentEvents = allEvents.Where(e => e.TransactionId == "0" || (e as Transaction)?.TransactionState == "Begin");

            //Split independent events up into buckets of 15 second intervals based on their Timestamp
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

                //Assign dependent events to their begin transaction
                var bucket = new List<Event>(group);
                foreach(var evt in allDependentEvents)
                {
                    if (startHash.Contains(evt.TransactionId))
                    {
                        startDict[evt.TransactionId].Events.Add(evt);                        
                    }                    
                }

                //Throw away any begin transactions that don't have anything associated with it
                trxStarts.RemoveAll(t => t.Events.Count == 0);

                //Order transaction's events by EventSequence and make sure they close
                foreach (var trx in trxStarts)
                {
                    trx.Events = trx.Events.OrderBy(e => e.EventSequence).ToList();
                    CloseTransactionIfOpen(trx);
                }
                buckets.Add(group.Key, bucket);              
            }

            //Identify orphaned events with no begin transaction
            var bucketDependentEvents = buckets.SelectMany(b => b.Value.Where(i => i is Transaction).SelectMany(i => (i as Transaction)?.Events));
            var orphanEvents = allDependentEvents.Except(bucketDependentEvents).ToList();
            foreach (var group in orphanEvents.GroupBy(e => e.TransactionId))
            {
                //Make orphans a begin transaction and order them by EventSequence
                var transaction = new Transaction
                {
                    Events = group.OrderBy(e => e.EventSequence).ToList()
                };
                var firstEvt = transaction.Events.First();
                transaction.TransactionId = firstEvt.TransactionId;
                transaction.TransactionState = "Begin";
                transaction.EventSequence = firstEvt.EventSequence - 1;
                transaction.Timestamp = firstEvt.Timestamp.AddMilliseconds(1);
                //Make sure this new transaction will close
                CloseTransactionIfOpen(transaction);

                //Find (or create) a bucket for this new transaction
                var key = transaction.Timestamp.ToString("ddhhmm") + transaction.Timestamp.Second / 15;
                if (buckets.TryGetValue(key, out var bucket))
                {
                    bucket.Add(transaction);
                }
                else
                {
                    buckets.Add(key, new List<Event> { transaction });
                }
            }

            //Order the buckets by key and all their items by Timestamp since EventSequence is only reliable within a transaction
            var orderedBuckets = buckets.OrderBy(b => b.Key).Select(b => b.Value.OrderBy(e => e.Timestamp).ToList()).ToList();       

            Console.WriteLine("Kicking off executions...");

            var tasks = new List<Task>();
            var eventExecutor = new EventExecutor();
            var replayOrigin = DateTimeOffset.UtcNow;

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

        private void CloseTransactionIfOpen(Transaction transaction)
        {
            //Close open transactions at end that don't include a commit or rollback
            if (transaction.Events.Any(e => (e as Transaction)?.TransactionState == "Commit" ||
                                            (e as Transaction)?.TransactionState == "Rollback")) return;
            var lastEvt = transaction.Events.Last();
            transaction.Events.Add(new Transaction
            {
                TransactionId = lastEvt.TransactionId,
                TransactionState = "Commit",
                EventSequence = lastEvt.EventSequence + 1,
                Timestamp = lastEvt.Timestamp.AddMilliseconds(1)
            });
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
