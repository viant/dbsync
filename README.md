# dbsync - SQL database cost effective synchronization


### Motivation

While there are many database solutions providing replication within the same vendor.
this project provides SQL based cross database vendor data synchronization.
You may easily synchronize small or large(billions+ records) tables/views in a cost effective way.
This is achieved by both determining the smallest changed dataset and by dividing dataset in the smaller chunks.


### Introduction

![synchronization diagram](dbsync.png)

##### 1. Synchronization status

In this step, synchronizer uses aggregation function to compare source and destination table.
In case of data discrepancy, the process narrows down source dataset to the one that has been changed.
When chunks are used, on top of narrowing source dataset, only out of sync chunks are transferred to destination database.

##### 2. Data transfer

Changed dataset is moved from source to transient table in destination database with transfer service.
Transfer service streamlines data copy with parallel writes and compacted collections.
On top of that large dataset can be divided in to smaller transferable chunks by sync process, which 
provides additional level of the read paralelization and 

##### 3. Data Merge

During checking synchronization status, sync process determines merge strategy based on the changed dataset
which can could be one of the following:

- insert - append data to destination table
- merge  - append or update data in destination table
- delete merge - remove data from destination table if it does not exist in transferred transient table, then merge
- delete insert - remove data from destination table if it does not exist in transferred transient table, then append


![synchronization process](process.png)


### Contract

######  Sync service contract

![dbsync contract](sync/contract.png)

###### Transfer service contract

<img src="transfer/contract.png" alt="transfer contract" width="40%">


### Usage



### Managing diff strategy

Detecting data discrepancy uses aggregate function on all or just specified columns.
Data comparision can be applied on the whole table, virtual partition(s) or chunk level.

By default all dest table columns are used to identified data discrepancy, 
the following aggregate function rules apply:
 - for any numeric data type SUM aggregate function is used
 - for any time/date data type MAX aggregate function is used
 - for other data type COUNT DISTINCT is used

When **countOnly** option is selected, total rows COUNT is used, this is especially useful when source
table uses data appends only.


In either case for single column ID based table the following aggregates are added:
 - max id: MAX(id) 
 - min id: MIN(id) 
 - total rows count: COUNT(1)
 - unique distinct count: COUNT(distinct ID) 
 - unique not null sum: SUM(CASE WHEN ID IS NULL THEN 1 ELSE 0 END) 

The last three are used to check if data inconsistency, duplication, id constraint violation.


##### Narrowing change dataset process

Narrowing process try to find max ID in destination dataset which is in sync with the source.
Note that this process is only applicable for single numeric ID based table.


######  Insert merge strategy

<img src="insert_strategy.png" alt="insert strategy" width="40%">

In case when source and dest dataset are discrepant and source ID is greater than dest ID, 
synchronizer takes dest max id, to check if up to that ID both dataset are equal, if so 
it uses INSERT strategy and transfer only source data where source ID is greater then dest max ID.


######  Update/Insert merge strategy

<img src="merge_strategy.png" alt="append discrepant" width="40%">
 
When source ID is greater then dest ID and [insert strategy](#insert-merge-strategy) can not be applied, 
synchronizer would try to reduce/expand dest dataset range where upper bound is limited by 
dest max ID and delta defined as half dataset ID distance (max id +/- delta),
if probed data is in sync, narrowed ID is used and delta is increased by half, otherwise decrease for next try.
Number of iteration in this process is controlled by depth parameter (0 by default).

When narrowed dataset is determined, merge(inser/update) strategy is used, 
and synchronizer transfers only source data where source ID is greater then narrowed ID.

###### Delete Merge strategy

<img src="delete_merge_strategy.png" alt="delete merge strategy" width="40%">

When source ID is lower than dest ID, or source row count is lower than dest, delete/merge strategy is used.
In case of non-chunked transfer all source dataset is copied to dest transient table, followed by deletion 
of any dest table records which are not be found in transient table, then data is merged.

When chunked-transfer is used only discrepant chunk are transferred, 
thus deletion is reduced to discrepant chunks.


###  Managing partition strategy
 
###  Managing chunk strategy

### Managing transfer

### Pseudo columns

### Non PK tables synchronization

### Applying custom filters

### Query based synchronization



### Running e2e tests



### Deployment
1. Standalone services
2. Docker compose
3. Cloud run
4. Kubernetes

### Supported database


### Custom build

