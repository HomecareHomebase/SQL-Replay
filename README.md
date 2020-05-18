# SQL-Replay
A tool for replaying events captured through SQL Server Extended Events

## Overview
The SQL Repay tool was created since SQL Server's Replay feature is based on SQL Server Profiler traces rather than SQL Server Extended Events.

Other key features of the SQL Replay tool:
- We don't want to replay events in a serial, synchronous manner. We want to simulate events firing concurrently at the same points in time as they took place in the event capture.
- We want to support bulk inserts. SQL Server Extended Events do not include the actual data bulk inserted, so the SQL Replay tool queries rows from the database during test prep to use to simulate captured bulk inserts.
- We want to replay stored procedure calls as remote procedure calls (RPC) rather than just replay the captured SQL statement representing the call as a SQL batch, which is important for preventing additional compilations and significantly inflating the number of compilations/sec.
- We want to facilitate synthetic generation of events for new stored procedures or existing stored procedures with parameter signature changes.

## Usage

### Prepping for Tests (from Extended Events Capture)

The SQL Replay tool executes binary data derived from Extended Events captures. The process of creating this binary data from an Extended Events capture is referred to as "prepping".

In order to prep Extended Events capture files, simply execute the SQL Replay tool from the command-line using the prep parameter.

```
SqlReplay.Console.exe prep C:\capture C:\capture\output 10 "Server=MYSERVER;Database=MYDATABASE;User Id=user;Password=password;Max Pool Size=1000;Connection Timeout=60;"
```

The first folder path is the location of the Extended Events capture files you wish to process.

The second folder path is the location in which you want to output the binary data derived from those Extended Events capture files. 

The 10 in this example is the number of binary data files you want to split the binary data into. Splitting the binary data into multiple files is key for distributing the calls across multiple processes to handle more traffic.

Next is a connection string to a database that matches the one you plan to test against in terms of code signatures. This connection string is used for two purposes:
1. Rather than just execute the same ad hoc SQL used to execute stored procedures captured through Extended Events, the SQL Replay tool uses stored procedure information from the database to build ADO.NET commands. This is important because the ad hoc SQL used to execute stored procedures artificially drives up compilations/sec.
2. The SQL Replay tool queries rows from the database to substitute for data not available in Extended Events captures of bulk inserts.
3. The connection string is embedded in the binary data and used when executing the replay tests--unless you override that by passing a new connection string when executing the tests. For more details on the properties used in the connection string in the example, please see the section on running tests.

### Prepping for Tests (New Stored Procedures or Existing Stored Procedures with Parameter Signature Changes)

The custom preprocessing feature generates a file of serialized objects comprised of sessions and their associates events and parameters, the same as the tool does normally. Instead of deriving binary data from Extended Events captures, custom preprocessing automatically creates these objects from the provided command-line arguments and user-defined parameters.

In order to generate custom file, execute the SQL Replay tool from the command-line using the prepnosc parameter.

```
SqlReplay.Console.exe prepnosc "C:\capture\" 1 155 "3/2/2020 7:30:00" "dbo.yourStoredProc" "C:\capture\params.json" 2
```

The first argument is the file path of the location at which the resulting file will be saved.

The second argument is the number Of Binary Files. The resulting output can be split into multiple files to allow for distributing the calls across multiple processes. However, one file should be sufficient as of now. Use a value of 1.

The third argument is the number of sessions to generate for the testing duration. Currently, the custom preprocessor only creates one event per session, so this can also be seen as the number of times your stored procedure will be called throughout the duration of testing (e.g. I want my proc called 400 times per hour during a 2 hour test, therefore the number of session I would like generated is 800).

The fourth argument is the start DateTime, the date and time in which the first event should be executed in a format that can be parsed as a valid DateTime value (e.g. "3/2/2020 7:30:00").

The fifth argument is the stored procedure name to be tested, with schema (e.g. “dbo.yourprocname").

The sixth argument is the file path of your parameter definition JSON file (see *Required Parameter Definition JSON File*  section below for more info).

The last argument is the run time in hours. This is the duration of time in which event execution will be evenly distributed. This parameter is optional and defaults to 2 hours. You should probably omit this parameter for now unless your circumstances require a duration other than 2 hours.

### Required Parameter Definition JSON File
Below is a sample parameter definition JSON file that defines two different parameters for a hypothetical stored procedure (the same file can be found in the CustomPreProcessor directory of this repo).

Each parameter definition is comprised of 4 sections:
1. ParameterName: The name of the parameter
2. AssignmentType: The method in which the custom preprocessor will assign the parameter a value from the pool in the Values section.

	The currently supported AssignmentTypes are:

	- Static: Uses the first value from the pool for each procedure call generated during custom preprocessing. If the parameter is nullable, an empty array can be provided and DBNull will be used.

	- Iterative: Loops through the pool in sequential fashion. Once the end is reached, the customer preprocessor will start again at the beginning of the list. This allow for more real-world procedure executions by virtue of value variance.

	- Random: Chooses a random value from the pool in a non-depleting fashion (i.e. randomly chosen values are not removed from the pool and could be chosen again).

3. Properties: The properties of the parameter which can be found by executing the following query within SSMS:

	```
	SELECT
		[Name]=p.[name],
		[Type]=type_name(p.system_type_id),
		[Length]=p.max_length,
		[Precision]=p.[precision],
		[Scale]=p.[scale],
		IsOutput=p.is_output,
		TypeName=schema_name(tt.schema_id) + '.' + type_name(tt.user_type_id),
		UserTypeId=tt.user_type_id,
		IsNullable=p.is_nullable
	FROM 
		sys.all_parameters p
	LEFT JOIN sys.table_types tt on tt.user_type_id = p.user_type_id
	WHERE object_id = object_id('your_procedure_name')
	ORDER BY parameter_id
	```
4. Values: The pool of values available for assignment during custom preprocessing. Can be left as an empty array if the parameter is nullable.

```
[
    {
      "ParameterName": "@param1",
      "AssignmentType": "Iterative",
      "Properties": {
          "DBType": "Int",
          "Size": 4,
          "Precision": 10,
          "Scale": 0,
          "Direction": "Input",
          "Value": null,
          "IsNullable": false
        },
        "Values": [
			1010,
			2020,
			3030,
			4040,
			5050,
			6060,
			7777,
			8888,
			9876,
			9999
        ]
    },
    {
      "ParameterName": "@param2",
      "AssignmentType": "Static",
      "Properties": {
          "DBType": "DateTime",
          "Size": 8,
          "Precision": 10,
          "Scale": 0,
          "Direction": "Input",
          "Value": null,
          "IsNullable": true
        },
    "Values": []
    }
  ]
```

### Warming Up

Prior to executing an actual run, you should warmup the database server to ensure that data is loaded into memory. To do so, simply execute the Replay tool from the command-line using the warmup parameter.

```
SqlReplay.Console.exe run C:\capture\output\replay0.txt 60 "dbo.usp_MyProcToInclude,dbo.usp_MyOtherProcToInclude" "Server=MYSERVER;Database=MYDATABASE;User Id=user;Password=password;Max Pool Size=1000;Connection Timeout=60;"
```

The file path is the path to a binary data file generated by the prepping process.

Next is the approximate duration in minutes that you want the warmup to last. If you provide a number less than 1 (or greater than the total time of the capture), the warmup will execute the entire set of captured events.

Next is a comma separated list of stored procedures, which should include their schema(s). These stored procedures will be the only ones included in the warmup. If you leave this as an empty string to, nothing will get run.

Last is an optional connection string which can be used to override the connection string embedded in the binary data file in case you want to test against a different SQL Server instance than the one used to prep the binary data file. For more details on the properties used in the connection string in the example, please see the next section on running tests.

Exceptions encountered during the execution of the warmup are held in memory and then dumped out into a log file corresponding to the binary data file being executed after all tests in that binary data file have been completed.

### Running Tests

In order to actually execute a run, simply execute the SQL Replay tool from the command-line using the run parameter.

```
SqlReplay.Console.exe run "C:\capture\output\replay0.txt" 0 "" "Server=MYSERVER;Database=MYDATABASE;User Id=user;Password=password;Max Pool Size=1000;Connection Timeout=60;"
```

The file path is the path to a binary data file generated by the prepping process.

Next is the approximate duration in minutes that you want the run to last. If you provide a number less than 1 (or greater than the total time of the capture), the run will execute the entire set of captured events.

Next is a comma separated list of stored procedures, which should include their schema(s). These stored procedures will be excluded from the run. Leave this as an empty string to not exclude anything.

Last is an optional connection string which can be used to override the connection string embedded in the binary data file in case you want to test against a different SQL Server instance than the one used to prep the binary data file. You'll notice that the example connection string sets max pool size to 1000 and connection timeout to 60. Max pool size is important for ensuring each process can open a sufficient number of connections to SQL Server at a time without overloading it. Connection timeout is important for ensuring that more time is given when trying to establish a connection to SQL Server than the default 15 seconds, which can be important to establishing SQL connections successfully under heavy load.

Exceptions encountered during the execution of a run are held in memory and then dumped out into a log file corresponding to the binary data file being run after all tests in that binary data file have been completed.

### Outputting Stored Procedure Calls

In order to output stored procedure calls to a JSON file, simply execute the SQL Replay tool from the command-line using the output parameter.

```
SqlReplay.Console.exe output C:\capture\output C:\capture\output\json\output.json "dbo.usp_MyStoredProcedure,dbo.usp_MyOtherStoredProcedure"
```

The folder path is the path to the binary data files generated by the prepping process.

Next is a file path for the JSON file you want to output.

The final parameter is a comma separated list of stored procedures, which should include their schema(s). These are the stored procedures, whose basic parameter data (name and value) will be included in the JSON file.