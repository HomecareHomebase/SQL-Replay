
namespace SqlReplay.Console
{
    internal class RunnerSettings: IRunnerSettings
    {
        /// <inheritdoc />
        public int SqlCommandTimeout { get; set; } = 1800;

        /// <inheritdoc />
        public int BulkCopyTimeout { get; set; } = 1800;

        /// <inheritdoc />
        public int TransactionScopeTimeout { get; set; } = 3600;

        /// <inheritdoc />
        public int BucketInterval { get; set; } = 15;

        /// <inheritdoc />
        public int StartDelayMinutes { get; set; } = 10;
    }
}
