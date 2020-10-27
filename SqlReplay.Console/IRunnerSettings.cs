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
    }
}
