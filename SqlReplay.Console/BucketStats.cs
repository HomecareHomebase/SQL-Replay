namespace SqlReplay.Console
{
    internal class BucketStats
    {
        public int timesFellBehind { get; set; }
        public double minDelayms { get; set; }
        public double maxDelayms { get; set; }
        // count of stored procedured called
        // list of stored procedures and their counts
    }
}