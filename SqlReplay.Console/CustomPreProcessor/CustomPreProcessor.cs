using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.IO;
using Newtonsoft.Json;

namespace SqlReplay.Console.CustomPreProcessing
{
    internal class CustomPreProcessor
    {
        private readonly int totalSessions;
        private readonly DateTimeOffset startDateTime;
        private readonly String storedProcedureName;
        private readonly ParameterLoader parameterLoader;
        private readonly Double eventTimeOffset;
        private readonly string jsonParameterFilePath;

        private const int secondsPerHour = 3600;
        private const int defaultRunTimeInHours = 2;

        private ConcurrentDictionary<string, Session> sessions;

        public CustomPreProcessor(int numberOfSessions, DateTime startDateTime, String storedProcName, String jsonParameterFilePath, int? runTimeInHours)
        {
            totalSessions = numberOfSessions;
            this.startDateTime = new DateTimeOffset(startDateTime, new TimeSpan(0, 0, 0));
            this.storedProcedureName = storedProcName;
            this.jsonParameterFilePath = jsonParameterFilePath;
            eventTimeOffset = (double)(secondsPerHour * (runTimeInHours ?? defaultRunTimeInHours)) / numberOfSessions;
            parameterLoader = GetParameterLoader();
        }

       
        public Run GenerateRun()
        {
            // Create Session, with RPC Event
            GenerateSessions();

            /* If a future update to this custom implementation allows for multiple events per session,
               uncomment the block below to ensure the events are sorted by EventSequence.
            */

            //foreach (Session session in sessions.Values)
            //{
            //    session.Events = session.Events.OrderBy(e => e.EventSequence).ToList();
            //}

            var run = new Run()
            {
                Sessions = sessions.Values.Where(s => s.Events.Count > 0).OrderBy(s => s.Events.First().Timestamp).ToList()
            };

            run.EventCaptureOrigin = run.Sessions.First().Events.First().Timestamp;

            return run;
        }

        private void GenerateSessions()
        {
            sessions = new ConcurrentDictionary<string, Session>();
            var timeStamp = startDateTime;

            long sessionCount = 1;
            while (sessionCount <= totalSessions)
            {
                var session_id = sessionCount.ToString();

                Session session = sessions.GetOrAdd(session_id, new Session() { SessionId = session_id });
                session.Events.Add(GetEvent(sessionCount, timeStamp));

                sessionCount++;
                timeStamp = timeStamp.AddSeconds(eventTimeOffset);
            }
        }

        private Event GetEvent(long sequence, DateTimeOffset timeStamp)
        {
            Event evt = null;

            evt = new Rpc()
            {
                EventSequence = sequence,
                TransactionId = "0", // Always 0 for now
                Procedure = storedProcedureName,
                Statement = null,
                ObjectName = null,
                Timestamp = timeStamp
            };

            parameterLoader.LoadParameters((Rpc)evt);

            return evt;
        }

        private ParameterLoader GetParameterLoader()
        {
            return new ParameterLoader(LoadJsonParameterInfo());
        }

        private IList<JsonParameterInfo> LoadJsonParameterInfo()
        {
            string jsonText = File.ReadAllText(jsonParameterFilePath);

            return JsonConvert.DeserializeObject<IList<JsonParameterInfo>>(jsonText);
        }
        
    }
}