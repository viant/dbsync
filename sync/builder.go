package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"sort"
	"strings"
	"time"
)

var dayTimeLayout = toolbox.DateFormatToLayout("yyyy-MM-dd")

type Builder struct {
	Sync
	pseudoColumns map[string]*PseudoColumn
	uniques       map[string]bool
	partitions    map[string]bool
	manager       dsc.Manager
	source        *Resource
	dest          *Resource
	dialect       dsc.DatastoreDialect
	table         string
	columns       []dsc.Column
	columnsByName map[string]dsc.Column
	datePartition string
}

func (b *Builder) Dialect() dsc.DatastoreDialect {
	if b.dialect != nil {
		return b.dialect
	}
	b.dialect = dsc.GetDatastoreDialect(b.manager.Config().DriverName)
	return b.dialect
}

func (b *Builder) HasDatePartition() bool {
	return b.datePartition != ""
}

func (b *Builder) Table(temp bool) string {
	if ! temp {
		return b.table
	}
	return b.table + "_tmp"
}

func (b *Builder) DDL(tempTable bool) (string, error) {
	dialect := b.Dialect()
	DDL, err := dialect.ShowCreateTable(b.manager, b.table)
	if err != nil {
		return "", err
	}
	if tempTable {
		DDL = strings.Replace(DDL, b.Table(false), b.Table(true), 1)
	}
	return DDL, nil
}

func (b *Builder) columnExpression(column string, resource *Resource) string {
	if resource.DateColumn != nil && resource.DateColumn.Name == column {
		return resource.DateColumn.Expression
	}
	if resource.HourColumn != nil && resource.HourColumn.Name == column {
		return resource.HourColumn.Expression
	}
	if pseudoColumn, ok := b.pseudoColumns[column]; ok {
		return pseudoColumn.Expression
	}
	return column
}

func (b *Builder) DQL(tempTable bool, resource *Resource, values map[string]interface{}) (string) {
	var projection = make([]string, 0)

	for _, column := range b.columns {
		if _, has := b.uniques[column.Name()]; has {
			continue
		}
		alias, ok := b.pseudoColumns[column.Name()]
		if ! ok {
			projection = append(projection, column.Name())
			continue
		}
		projection = append(projection, fmt.Sprintf("%v AS %v", alias, column.Name()))
	}
	if len(b.UniqueColumns) > 0 {
		projection = append(b.UniqueColumns, projection...)
	}
	DQL := fmt.Sprintf("SELECT %v\nFROM %v t", strings.Join(projection, ",\n"), b.Table(tempTable))
	if len(values) > 0 {
		var whereCritera = make([]string, 0)
		for k, v := range values {
			column := b.columnExpression(k, resource)
			whereCritera = append(whereCritera, criteria(column, v))
		}
		DQL += "\nWHERE " + strings.Join(whereCritera, " AND ")
	}
	return DQL
}

func (b *Builder) init(manager dsc.Manager) error {
	b.manager = manager
	b.dialect = dsc.GetDatastoreDialect(manager.Config().DriverName)
	datastore, err := b.dialect.GetCurrentDatastore(manager)
	if err != nil {
		return err
	}
	b.uniques = make(map[string]bool)
	if len(b.UniqueColumns) == 0 {
		b.UniqueColumns = make([]string, 0)
	}
	for _, column := range b.UniqueColumns {
		b.uniques[column] = true
	}
	b.pseudoColumns = make(map[string]*PseudoColumn)
	if len(b.PseudoColumns) == 0 {
		b.PseudoColumns = make([]*PseudoColumn, 0)
	}
	for _, column := range b.PseudoColumns {
		b.pseudoColumns[column.Name] = column
	}
	b.partitions = make(map[string]bool)

	for _, partition := range b.Partition.Columns {
		b.partitions[partition] = true
	}
	if b.columns, err = b.dialect.GetColumns(manager, datastore, b.table); err != nil {
		return err
	}
	for _, column := range b.columns {
		b.columnsByName[column.Name()] = column
	}
	return nil
}

func (b *Builder) initDatePartition(resource *Resource) {
	if len(b.Partition.Columns) > 0 {
		dateColumn := resource.DateColumn
		hasDateColumn := dateColumn != nil
		for _, partition := range b.Partition.Columns {
			if hasDateColumn && dateColumn.Name == partition {
				b.datePartition = fmt.Sprintf("%v AS %v", dateColumn.Expression, dateColumn.Name)
				continue
			}
			column, ok := b.columnsByName[partition]
			if ! ok {
				continue
			}
			switch strings.ToUpper(column.DatabaseTypeName()) {
			case "DATE", "TIME", "DATETIME", "TIMESTAMP":
				b.datePartition = partition
			}
		}
	}
}

func (b *Builder) DiffDQL(time time.Time, resource *Resource) (string, []string) {

	return b.diffDQL(time, resource, func(projection *[]string, dimension map[string]bool) {
		for _, column := range b.columns {

			if _, has := dimension[column.Name()]; has {
				continue
			}
			switch strings.ToUpper(column.DatabaseTypeName()) {
			case "FLOAT", "NUMERIC", "DECIMAL", "FLOAT64":
				*projection = append(*projection, fmt.Sprintf("ROUND(SUM(COALESCE(%v, 0)), 2) AS sum_%v", column.Name(), column.Name()))
				continue
			case "TIMESTAMP", "TIME", "DATE", "DATETIME":
				*projection = append(*projection, fmt.Sprintf("MAX(%v) AS max_%v", column.Name(), column.Name()))
			default:
				*projection = append(*projection, fmt.Sprintf("COUNT(DISTINCT COALESCE(%v, 0)) AS cnt_%v", column.Name(), column.Name()))
			}
		}
	})
}


func (b *Builder) CountDiffDQL(time time.Time, resource *Resource) (string, []string) {
	return b.diffDQL(time, resource, func(projection *[]string, dimension map[string]bool) {
		*projection = append(*projection, "COUNT(1) AS cnt")
	})
}

func (b *Builder) diffDQL(time time.Time, resource *Resource, projectionGenerator func(projection *[]string, dimension map[string]bool)) (string, []string) {
	var projection = make([]string, 0)
	var groupBy = make([]string, 0)
	var whereClause = make([]string, 0)
	groupByIndex := 1
	var dimension = make(map[string]bool)
	if column := resource.DateColumn; column != nil {
		dimension[column.Name] = true
		projection = append(projection, fmt.Sprintf("%v AS %v", column.Expression, column.Name))
		groupBy = append(groupBy, toolbox.AsString(groupByIndex))
		groupByIndex++
		whereClause = append(whereClause, fmt.Sprintf("%v = '%v'", column.Expression, time.Format(dayTimeLayout)))
	}
	if column := resource.HourColumn; column != nil {
		dimension[column.Name] = true
		projection = append(projection, fmt.Sprintf("%v AS %v", column.Expression, column.Name))
		groupBy = append(groupBy, toolbox.AsString(groupByIndex))
		groupByIndex++
	}
	for _, partition := range b.Partition.Columns {
		if _, has := dimension[partition]; has {
			continue
		}
		dimension[partition] = true
		projection = append(projection, partition)
		groupBy = append(groupBy, toolbox.AsString(groupByIndex))
		groupByIndex++
	}

	projectionGenerator(&projection, dimension)

	SQL := fmt.Sprintf("SELECT %v\nFROM %s", strings.Join(projection, ",\n\t"), b.table)
	if len(whereClause) > 0 {
		SQL += fmt.Sprintf("\nWHERE %s", strings.Join(whereClause, " AND "))
	}
	if len(groupBy) > 0 {
		SQL += fmt.Sprintf("\nGROUP BY %s", strings.Join(groupBy, ","))
	}
	if len(groupBy) > 0 {
		SQL += fmt.Sprintf("\nORDER BY %s", strings.Join(groupBy, ","))
	}
	return SQL, toolbox.MapKeysToStringSlice(dimension)
}

func (b *Builder) DML(dmlType string, filter map[string]interface{}) (string, error) {
	switch dmlType {
	case DMLInsertReplace:
		return b.insertReplaceDML(filter), nil
	case DMLInsertUpddate:
		return b.insertUpdateDML(filter), nil
	case DMLMerge:
		return b.mergeDML(filter), nil
	case DMLInsert:
		return b.insertDML(filter), nil
	case DMLDelete:
		return b.deleteDML(filter), nil
	}
	return "", fmt.Errorf("unsupported %v", dmlType)
}

func (b *Builder) insertNameAndValues() (string, string) {
	var names = make([]string, 0)
	var values = make([]string, 0)
	for _, column := range b.UniqueColumns {
		names = append(names, column)
		values = append(values, b.columnExpression(column, b.dest))
	}
	for _, column := range b.columns {
		if _, ok := b.uniques[column.Name()]; ok {
			continue
		}
		names = append(names, column.Name())
		values = append(values, b.columnExpression(column.Name(), b.dest))
	}
	return strings.Join(names, ","), strings.Join(values, ",")
}

func (b *Builder) updateSetValues() (string) {
	update := make([]string, 0)
	for _, column := range b.columns {
		if b.uniques[column.Name()] {
			continue
		}
		value := b.columnExpression(column.Name(), b.dest)
		if value == column.Name() {
			value = "t." + column.Name()
		}
		update = append(update, fmt.Sprintf("%v = %v", column.Name(), value))
	}
	return strings.Join(update, ",\n\t")
}

func (b *Builder) baseInsert(withReplace bool) string {
	names, values := b.insertNameAndValues()
	replace := ""
	if withReplace {
		replace = "OR REPLACE"
	}
	return fmt.Sprintf("INSERT %v INTO %v(%v) SELECT %v FROM %v t", replace, b.Table(false), names, values, b.Table(true))
}

func (b *Builder) insertUpdateDML(filter map[string]interface{}) string {
	DML := b.baseInsert(false)
	return fmt.Sprintf("%v \nON DUPLICATE KEY \n UPDATE %v", DML, b.updateSetValues())
}

const mergeSQL = `
MERGE %v d
USING %v t
ON %v
WHEN MATCHED THEN
  UPDATE SET %v
WHEN NOT MATCHED BY TARGET THEN
  INSERT (%v)
  VALUES(%v)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
`

func (b *Builder) mergeDML(filter map[string]interface{}) (string) {
	DQL := b.DQL(true, b.dest, nil)
	var onCriteria = make([]string, 0)
	for _, column := range b.UniqueColumns {
		onCriteria = append(onCriteria, fmt.Sprintf("d.%v = t.%v", column, column))
	}
	filterKeys := toolbox.MapKeysToStringSlice(filter)
	sort.Strings(filterKeys)
	for _, k := range filterKeys {
		v := filter[k]
		onCriteria = append(onCriteria, criteria("d."+k, v))
	}
	setValues := b.updateSetValues()
	names, values := b.insertNameAndValues()
	return fmt.Sprintf(mergeSQL,
		b.Table(false),
		fmt.Sprintf("(%v)", DQL),
		strings.Join(onCriteria, " AND "),
		setValues,
		names, values)
}

func (b *Builder) getInsertWhereClause(filter map[string]interface{}) string {
	if len(b.UniqueColumns) > 0 {
		innerWhere := ""
		var innerCriteria = make([]string, 0)
		filterKeys := toolbox.MapKeysToStringSlice(filter)
		sort.Strings(filterKeys)
		for _, k := range filterKeys {
			v := filter[k]
			innerCriteria = append(innerCriteria, criteria("t."+k, v))
		}
		if len(innerCriteria) > 0 {
			innerWhere = "WHERE  " + strings.Join(innerCriteria, " AND ")
		}
		inCriteria := fmt.Sprintf("(%v) NOT IN (SELECT %v FROM %v t %v)", strings.Join(b.UniqueColumns, ","), strings.Join(b.UniqueColumns, ","), b.Table(false), innerWhere)
		return "\nWHERE " + inCriteria
	}
	return ""
}

func (b *Builder) insertDML(filter map[string]interface{}) string {
	return b.baseInsert(false) + b.getInsertWhereClause(filter)
}

func (b *Builder) insertReplaceDML(filter map[string]interface{}) string {
	return b.baseInsert(true) + b.getInsertWhereClause(filter)
}

func (b *Builder) deleteDML(filter map[string]interface{}) string {
	var whereClause = make([]string, 0)
	filterKeys := toolbox.MapKeysToStringSlice(filter)
	sort.Strings(filterKeys)
	for _, k := range filterKeys {
		v := filter[k]
		whereClause = append(whereClause, criteria("d."+k, v))
	}
	if len(b.UniqueColumns) > 0 {
		inCriteria := fmt.Sprintf("(%v) NOT IN (SELECT %v FROM %v)", strings.Join(b.UniqueColumns, ","), strings.Join(b.UniqueColumns, ","), b.Table(true))
		whereClause = append(whereClause, inCriteria)
	}
	return fmt.Sprintf("DELETE FROM %v WEHRE  %v", b.Table(false), strings.Join(whereClause, " AND "))
}

func NewBuilder(request *SyncRequest) (*Builder, error) {
	builder := &Builder{
		Sync:          request.Sync,
		columns:       make([]dsc.Column, 0),
		columnsByName: make(map[string]dsc.Column),
		table:         request.Dest.Table,
		source:        request.Source,
		dest:          request.Dest,
	}
	manager, err := dsc.NewManagerFactory().Create(request.Dest.Config);
	if err != nil {
		return nil, err
	}
	if err = builder.init(manager); err != nil {
		return nil, err
	}
	builder.initDatePartition(request.Dest)
	return builder, nil
}
