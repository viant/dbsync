## July 9 2019 - v0.11.0
- Added columns to explicitly control sync columns
- Patched index out of bound
- Updated schedulable ID with 4 URL parts


## July 8 2019 - v0.10.0
- Added comparator logging
- Patched time comparision
- Added RecentErrors "immediate" retry 

## July 2 2019 - v0.9.11
- Patched empty source partition case 

## June 28 2019 - v0.9.10
- Patched error status cleanup
- Patched race condition


## June 20 2019 - v0.9.7
- Patched missing partition filter on chunked batc
- Patched appendOnly deleteMerge case
- Added dest integrity validation
- Minor errors handling refactoring 


## June 20 2019 - v0.9.6
- Patched upper casing aliases
- Patched nil pointer on status
- Updated status to include transfers
- Added dest signature validation

## June 20 2019 - v0.9.1
- Patched default ID on schedulables
- Added start/end job info logging
- Patched date formatting

## June 18 2019 - v0.9.0
- Major refactoring/restructuring
- Added cross partition sync (with removal)
- Added diff.NewIDOnly strategy
- Added DirectAppend option (to bypass transient table for inserts/streaming)

## May 21 2019 - v0.8.4
- Patch date logging
- Updated diff logging

## May 21 2019 - v0.8.2
- Patched schedule.AtTime with TZ
- Patched chunking with source continues streaming new data
- Patched upper case aliasing

## May 21 2019 - v0.8.0
- Added schedule.AtTime with TZ support scheduling option

## May 20 2019 - v0.6.3
- Updated partition batching to reduce number of SQL statements

## May 16 2019 - v0.6.1
- Added pseudo column aggregation
- Updated coalesce default value to ' ' (space)  as oracle xe COUNT(DISTINCT COALESCE(x, '')) returns 0 with '' and 1 with ' ' where x has all rows with nulls

## May 15 2019 - v0.5.0
- Added sync mode to control partition/chunk batch or individual sync with dest table

## May 14 2019 - v0.4.1
- Disabled scheduler is scheduleURL is empty
- Patched multi partition value batching optimization

## May 13 2019 - v0.4.0
- Moved chunk.SQL to resource.ChunkSQL

## May 9 2019 - v0.3.1
- Added mod time based schedule reloading
- Added DDLFromSelect 
- Renamed URI scheduled to schedules
- Patched multi directory schedules loading  

## May 8 2019 - v0.2.1
- Added schedule.Disabled to control active/in active schedules  

## May 7 2019 - v0.2.0
- Updated scheduler to scan schedule url with sub folders 
- Patch nil pointer

