CREATE EVENT SESSION [XECapture_SQL_Replay] ON SERVER 
ADD EVENT sqlserver.rpc_starting(SET collect_data_stream=(1),collect_statement=(1)
    ACTION(package0.collect_current_thread_id,package0.event_sequence,sqlos.cpu_id,sqlos.scheduler_id,sqlos.system_thread_id,sqlos.task_address,sqlos.worker_address,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.is_system,sqlserver.plan_handle,sqlserver.request_id,sqlserver.session_id,sqlserver.transaction_id,sqlserver.username)),
ADD EVENT sqlserver.sql_batch_completed(SET collect_batch_text=(1)
    ACTION(package0.collect_current_thread_id,package0.event_sequence,sqlos.cpu_id,sqlos.scheduler_id,sqlos.system_thread_id,sqlos.task_address,sqlos.worker_address,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.is_system,sqlserver.plan_handle,sqlserver.request_id,sqlserver.session_id,sqlserver.transaction_id,sqlserver.username)),
ADD EVENT sqlserver.sql_batch_starting(SET collect_batch_text=(1)
    ACTION(package0.collect_current_thread_id,package0.event_sequence,sqlos.cpu_id,sqlos.scheduler_id,sqlos.system_thread_id,sqlos.task_address,sqlos.worker_address,sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.is_system,sqlserver.plan_handle,sqlserver.request_id,sqlserver.session_id,sqlserver.transaction_id,sqlserver.username)),
ADD EVENT sqlserver.sql_transaction(
    ACTION(package0.collect_current_thread_id,package0.event_sequence,sqlos.cpu_id,sqlos.scheduler_id,sqlos.system_thread_id,sqlos.task_address,sqlos.worker_address,sqlserver.database_id,sqlserver.database_name,sqlserver.is_system,sqlserver.plan_handle,sqlserver.request_id,sqlserver.session_id,sqlserver.transaction_id)
    WHERE ([transaction_type]=(1)))
ADD TARGET package0.event_file(SET filename=N'C:\XECapture\XECaptureSqlReplay.xel',max_file_size=(10240),max_rollover_files=(15))
WITH (MAX_MEMORY=40960 KB,EVENT_RETENTION_MODE=NO_EVENT_LOSS,MAX_DISPATCH_LATENCY=0 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=PER_CPU,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
