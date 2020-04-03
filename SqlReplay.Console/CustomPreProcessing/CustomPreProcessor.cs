using System;
using System.Collections.Generic;
using System.Data;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;

namespace SqlReplay.Console.CustomPreProcessing
{
    internal class CustomPreProcessor
    {
        private readonly int totalSessions;
        private readonly DateTimeOffset startDateTime;
        private readonly String spName;
        private readonly IParameterLoader parmLoader;
        private readonly Double eventTimeOffset;

        private const String parmLoaderNamespace = "SqlReplay.Console.CustomRuns.ParameterLoaders";
        private const int secondsPerHour = 3600;
        private const int defaultRunTimeInHours = 2;

        private ConcurrentDictionary<string, Session> sessions;

        public CustomPreProcessor(int numberOfSessions, DateTime runStartDateTime, String storedProcName, String parameterLoader, int? runTimeInHours)
        {
            totalSessions = numberOfSessions;
            startDateTime = new DateTimeOffset(runStartDateTime, new TimeSpan(1, 0, 0));
            eventTimeOffset = (double)(secondsPerHour * (runTimeInHours ?? defaultRunTimeInHours)) / numberOfSessions;
            spName = storedProcName;
            parmLoader = getParameterLoader(parameterLoader);
        }

        private IParameterLoader getParameterLoader(String parameterLoader)
        {
            var loaderType = Assembly.GetExecutingAssembly().GetType(parmLoaderNamespace + "." + parameterLoader);

            return (IParameterLoader)Activator.CreateInstance(loaderType);
        }

        public Run GenerateRun()
        {
            // Create Session, with RPC Event, 
            GenerateSessions();
            
            foreach (Session session in sessions.Values)
            {
                session.Events = session.Events.OrderBy(e => e.EventSequence).ToList();
            }

            var run = new Run()
            {
                Sessions = sessions.Values.Where(s => s.Events.Count > 0).OrderBy(s => s.Events.First().EventSequence).ToList()
            };

            run.EventCaptureOrigin = run.Sessions.First().Events.First().Timestamp;

            return run;
        }

        private void GenerateSessions()
        {
            sessions = new ConcurrentDictionary<string, Session>();
            var timeStamp = startDateTime;

            int sessionCount = 1;
            while (sessionCount <= totalSessions)
            {
                var session_id = sessionCount.ToString();

                Session session = sessions.GetOrAdd(session_id, new Session() { SessionId = session_id });
                session.Events.Add(GetEvent(sessionCount, timeStamp));

                sessionCount++;
                timeStamp = timeStamp.AddSeconds(eventTimeOffset);
            }
        }

        private Event GetEvent(int sequence, DateTimeOffset timeStamp)
        {
            Event evt = null;

            evt = new Rpc()
            {
                EventSequence = sequence.ToString(),
                TransactionId = "0", // Always 0 for now
                Procedure = spName,
                Statement = null,
                ObjectName = null,
                Timestamp = timeStamp
            };

            parmLoader.LoadParameters((Rpc)evt);

            return evt;
        }
    }
}