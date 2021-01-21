namespace SqlReplay.Batch
{
    class BatchConfig
    {
        public string ReplayConsolePath { get; set; }

        public string ConnectionString { get; set; }

        public string OutputPath { get; set; }

        public Batch[] Batches { get; set; }

        public int StatusCheckIntervalInSec { get; set; }
    }

    class Batch
    {
        public int ProcessingOrder { get; set; }

        public string BatchId { get; set; }

        public int NumOfInstances { get; set; }

        public string StartAt { get; set; }

        public int DurationInMinutes { get; set; }

        public string[] InputFiles { get; set; }
    }
}

