namespace SqlReplay.Console
{
    internal class BucketStats
    {
        public int timesFellBehind { get; set; }
        public double minDelayms { get; set; }
        public double maxDelayms { get; set; }
    }
}