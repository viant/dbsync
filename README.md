# dbsync - SQL database cost effective synchronization


### Motivation

While there are many database solutions providing replication within the same vendor,
this project provides SQL based cross database vendor data synchronization.
You may easily synchronize small or large(billions+ records) tables/views in a cost effective way.
This is achieved by both determining the smallest changed dataset and by dividing dataset in the partition or smaller chunks.


### Introduction

![synchronization diagram](dbsync.png)

##### 1. Synchronization status

In this step, synchronizer uses aggregation function to compare source and destination table.
In case of data discrepancy, the process narrows down source dataset to the one that has been changed.
When chunks or partition or both are used, on top of narrowing source dataset, only out of sync data segments are transferred to destination database.

##### 2. Data transfer

Changed dataset is moved from source to transient table in destination database with transfer service.
Transfer service streamlines data copy with parallel writes and compacted collections.
It uses batched inserts or batched load job (BigQuery) to reduce unnecessary round trips.
On top of that large dataset can be divided in to partition or/and smaller transferable chunks, which 
provides additional level of the read paralelization. 

##### 3. Data Merge

During checking synchronization status, sync process determines merge strategy based on the changed dataset
which is one of the following:

- insert - append data to destination table
- merge  - append or update data in destination table
- delete merge - remove data from destination table if it does not exist in transferred transient table, then merge
- delete insert - remove data from destination table if it does not exist in transferred transient table, then append


![synchronization process](process.png)


### Contract

######  Sync service contract

![dbsync contract](sync/contract.png)

##### Sync service configuration 
* --url URL location with scheduled synchronization
* --urlRefresh scehdule location refresh in ms
* --port service port
* --debug enabled debug
* --statsHistory number of completed sync info stored in memory (/v1/api/history/{ID})

###### Transfer service contract

<img src="transfer/contract.png" alt="transfer contract" width="40%">




### Usage


##### On demand with JSON request

```bash
curl -X POST -d=@request.json http://syncHost:8081/v1/api/sync
```

[data.json](usage/request.json)
```json
{
    "Table": "events",
	"IDColumns": [
		"id"
	],
    "Source": {
        "Credentials": "mysql-e2e",
        "Descriptor": "[username]:[password]@tcp(127.0.0.1:3306)/[dbname]?parseTime=true",
        "DriverName": "mysql",
        "Parameters": {
            "dbname": "db1"
        }
    },
	"Dest": {
		"Credentials": "mysql-e2e",
		"Descriptor": "[username]:[password]@tcp(127.0.0.1:3306)/[dbname]?parseTime=true",
		"DriverName": "mysql",
		"Parameters": {
			"dbname": "db2"
		}
	},
	"Transfer": {
    	"EndpointIP": "127.0.0.1:8080",
	    "WriterThreads": 2,
    	"BatchSize": 2048
	}
}

```

##### On demand with YAML request

```bash
curl -X POST -H=Content-Type=app/yaml -d=@request.json http://syncHost:8081/v1/api/sync
```

[request.yaml](usage/request.yaml)
```yaml
table: events
idColumns: 
  - id
source: 
  credentials: mysql-e2e
  descriptor: [username]:[password]@tcp(127.0.0.1:3306)/[dbname]?parseTime=true
  driverName: mysql
  parameters: 
    dbname: db1
dest: 
  credentials: mysql-e2e
  descriptor: [username]:[password]@tcp(127.0.0.1:3306)/[dbname]?parseTime=true
  driverName: mysql
  parameters: 
    dbname: db2
    
transfer: 
  endpointIP: 127.0.0.1:8080
  writerThreads: 2
  batchSize: 2048

```


##### Scheduled sync 

Place scheduled request in folder

[scheduled.yaml](usage/scheduled.yaml)
```yaml
table: events
idColumns:
  - id
source:
  ...
dest:
   ...
transfer:
  endpointIP: 127.0.0.1:8080
  batchSize: 2048
  writerThreads: 2
diff:
  countOnly: true  
schedule:
  frequency:
    value: 1
    unit: hour
```


### Data comparision strategy

Detecting data discrepancy uses aggregate function on all or just specified columns.
Data comparision can be applied on the whole table, virtual partition(s) or a chunk level.

By default all dest table columns are used to identified data discrepancy, 
the following aggregate function rules apply:
 - for any numeric data type SUM aggregate function is used
 - for any time/date data type MAX aggregate function is used
 - for other data type COUNT DISTINCT is used

When **countOnly** option is selected, total rows COUNT is used, this is especially useful when source
table uses data appends only.


In either case for single column ID based table the following aggregates are added:
 - max ID: MAX(id) 
 - min ID: MIN(id) 
 - total rows count: COUNT(1)
 - unique distinct count: COUNT(distinct ID) 
 - unique not null sum: SUM(CASE WHEN ID IS NULL THEN 1 ELSE 0 END) 

The last three are used to check if data inconsistency, duplication, id constraint violation.


##### Narrowing change dataset process

Narrowing process try to find max ID in destination dataset which is in sync with the source.
Note that this process is only applicable for single numeric ID based table.


######  Insert strategy

<img src="insert_strategy.png" alt="insert strategy" width="40%">

In case when source and dest dataset are discrepant and source ID is greater than dest ID, 
synchronizer takes dest max ID, to check if up to that ID both dataset are equal, if so 
it uses INSERT strategy and transfer only source data where source ID is greater then dest max ID.


######  Merge strategy

<img src="merge_strategy.png" alt="append discrepant" width="40%">
 
When source ID is greater then dest ID and [insert strategy](#insert-strategy) can not be applied, 
synchronizer would try to reduce/expand dest dataset range where upper bound is limited by 
dest max ID and delta defined as half dataset ID distance (max id +/- delta),
if probed data is in sync, narrowed ID is used and delta is increased by half, otherwise decrease for next try.
Number of iteration in this process is controlled by depth parameter (0 by default).

When narrowed dataset is determined, merge(inser/update) strategy is used, 
and synchronizer transfers only source data where source ID is greater then narrowed ID.

###### Delete/Merge strategy

<img src="delete_merge_strategy.png" alt="delete merge strategy" width="40%">

When source ID is lower than dest ID, or source row count is lower than dest, delete/merge strategy is used.
In case of non-chunked transfer all source dataset is copied to dest transient table, followed by deletion 
of any dest table records which are not be found in transient table, then data is merged.

When chunked-transfer is used only discrepant chunk are transferred, 
thus deletion is reduced to discrepant chunks.


###### Diff Contract settings
- diff.columns: custom columns with aggregate function used to compare source and destination
- diff.countOnly: flag to use only row COUNT comparision
- diff.depth:  specifies number attempts to find max dest ID synchronized with the source
- diff.batchSize: number of partition  (512)
- diff.numericPrecision: default required decimal precision when comparing decimal data (can be also specified on diff.columns level)
- diff.dateFormat: default date format used to compare date/time data type (can be also specified on diff.columns level)
- diff.dateLayout: default date layout used to compare date/time data type (can be also specified on diff.columns level)


##### Custom data comparision

In scenario where each source data mutation also updates specified column(s) i.e UPDATED, it is more effective to just use
column(s) in question instead of all of them. 
Note that, since table is single ID based beside COUNT(1), MAX(ID), MIN(ID) is also used in data comparision.

[custom_diff.yaml](usage/custom_diff.yaml)

```yaml
table: sites
idColumns:
  - id
source:
  ...
dest:
  ...
  
diff:
  depth: 2
  columns:
    - name: UPDATED
      func: MAX
```


###  Partitioned synchronization

Partition synchronization uses partition values as filter to divide dataset into a smaller partition bound
segments. Partition column values are generated by a sqlProvider. 
Only out of syn partition are synchronized, diff computation could be batched into one SQL for number of parition
which is controlled with batchSize on strategy diff level (512 by default).

While each discrepant partition is synchronized individually, multiple partition can be processed concurrently which 
is controlled with threads strategy  partition level setting (2 by default) 



###  Chunk

Chunk synchronization uses ID based range to divide a dataset into a smaller segments.
Range span is controlled with   


### Pseudo columns


### Non PK tables synchronization

### Applying custom filters

### Query based synchronization

### Managing transfer


### ORDER/GROUP BY position support

Some dataase do not support GROUP/ORDER BY position, in that case actual unaliased expression has to be used
resource.positionReference informs query builder if databae vendor support this option.
 
- source.positionReference flags if source database support GROUP/ORDER BY position
- dest.positionReference flags if dest database support GROUP/ORDER BY position

### Running e2e tests



### Deployment
1. Standalone services
2. Docker compose
3. Cloud run - TODO provide examples
4. Kubernetes - TODO provide examples

### Supported database
  - all drivers supporting database/sql

#### Default build RDBMS drivers
  - BigQuery
  - Oracle
  - MySQL
  - Postgress
  - Vertica 
  - ODBC

### Custom build
- TODO 


### Checking data sync quality

 