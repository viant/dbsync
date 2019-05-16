## May 16 2019 - v0.6.0
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

