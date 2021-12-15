namespace SqlReplay.Console
{
    internal interface IRunnerSettings
    {
        /// <summary>
        /// Defines Sql command timeout value.
        /// </summary>
        int SqlCommandTimeout { get; set; }

        /// <summary>
        /// Defines timeout value for bulk copy.
        /// </summary>
        int BulkCopyTimeout { get; set; }

        /// <summary>
        /// Defines timeout value for transaction scope.
        /// </summary>
        int TransactionScopeTimeout { get; set; }

        /// <summary>
        /// Defines bucket interval used for grouping on sessions.
        /// </summary>
        int BucketInterval { get; set; }

        /// <summary>
        /// Defines time in minutes for the process to wait before starting. This helps keep processes in sync.
        /// </summary>
        public int StartDelayMinutes { get; set; }
    }
}
