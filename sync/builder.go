package sync

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"sort"
	"strings"
)


type Builder struct {
	Sync
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


func (b *Builder) Table(suffix string) string {
	if suffix == ""{
		return b.table
	}
	return b.table + suffix
}



func (b *Builder) DDL(tempTable string) (string, error) {
	dialect := b.Dialect()
	DDL, err := dialect.ShowCreateTable(b.manager, b.table)
	if err != nil {
		return "", err
	}
	if tempTable != "" {
		DDL = strings.Replace(DDL, b.Table(""), b.Table(tempTable), 1)
	}
	return DDL, nil
}

func (b *Builder) columnExpression(column string, resource *Resource) string {
	if pseudoColumn, ok := resource.pseudoColumns[column]; ok {
		return pseudoColumn.Expression
	}
	return column
}

func (b *Builder) ChunkDQL(resource *Resource, offset, limit int, values map[string]interface{}) (string, error) {
	state := data.NewMap()
	state.Put("hint", resource.Hint)
	state.Put( "offset", offset)
	state.Put( "max", limit + offset)
	state.Put( "limit", limit)
	state.Put( "where", "")
	state.Put( "whereClause", "")
	if len(values) > 0 {
		var whereCritera = make([]string, 0)
		for k, v := range values {
			column := b.columnExpression(k, resource)
			whereCritera = append(whereCritera, toCriterion(column, v))
		}
		state.Put( "where", " AND " + strings.Join(whereCritera, " AND "))
		state.Put( "whereClause", " WHERE " + strings.Join(whereCritera, " AND "))
	}
	chunkSQL := b.Sync.ChunkSQL
	if chunkSQL == "" {
		chunkSQL = b.defaultChunkDQL()
	}
	DQL := state.ExpandAsText(chunkSQL)
	return DQL, nil
}



func (b *Builder) CountDQL(suffix string, resource *Resource, criteria map[string]interface{}) (string) {
	DQL := fmt.Sprintf("SELECT COUNT(1) AS count_value\nFROM %v t", b.Table(suffix))
	if len(criteria) > 0 {
		var whereCritera = make([]string, 0)
		keys := toolbox.MapKeysToStringSlice(criteria)
		sort.Strings(keys)
		for _, k := range keys {
			v := criteria[k]
			column := b.columnExpression(k, resource)
			whereCritera = append(whereCritera, toCriterion(column, v))
		}
		DQL += "\nWHERE " + strings.Join(whereCritera, " AND ")
	}
	return DQL
}

func (b *Builder) DQL(suffix string, resource *Resource, values map[string]interface{}) (string) {
	var projection = make([]string, 0)

	for _, column := range b.columns {
		if _, has := b.uniques[column.Name()]; has {
			continue
		}
		alias, ok := resource.pseudoColumns[column.Name()]
		if ! ok {
			projection = append(projection, column.Name())
			continue
		}
		projection = append(projection, fmt.Sprintf("%v AS %v", alias, column.Name()))
	}
	if len(b.UniqueColumns) > 0 {
		projection = append(b.UniqueColumns, projection...)
	}
	DQL := fmt.Sprintf("SELECT %v %v\nFROM %v t", resource.Hint, strings.Join(projection, ",\n"), b.Table(suffix))
	if len(values) > 0 {
		var whereCritera = make([]string, 0)
		keys := toolbox.MapKeysToStringSlice(values)
		sort.Strings(keys)
		for _, k := range keys {
			v := values[k]
			column := b.columnExpression(k, resource)
			whereCritera = append(whereCritera, toCriterion(column, v))
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
	b.source.indexPseudoColumns()
	b.dest.indexPseudoColumns()
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



func (b *Builder) DiffDQL(criteria map[string]interface{}, resource *Resource) (string, []string) {
	return b.diffDQL(criteria, resource, func(projection *[]string, dimension map[string]bool) {
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

func (b *Builder) CountDiffDQL(criteria map[string]interface{}, resource *Resource) (string, []string) {
	return b.diffDQL(criteria, resource, func(projection *[]string, dimension map[string]bool) {
		*projection = append(*projection, "COUNT(1) AS cnt")
	})
}

func (b *Builder) diffDQL(criteria map[string]interface{}, resource *Resource, projectionGenerator func(projection *[]string, dimension map[string]bool)) (string, []string) {
	var projection = make([]string, 0)
	var groupBy = make([]string, 0)
	var whereClause = make([]string, 0)
	groupByIndex := 1
	var dimension = make(map[string]bool)

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

func (b *Builder) DML(dmlType string, suffix string, filter map[string]interface{}) (string, error) {
	switch dmlType {
	case DMLInsertReplace:
		return b.insertReplaceDML(suffix, filter), nil
	case DMLInsertUpddate:
		return b.insertUpdateDML(suffix, filter), nil
	case DMLMerge:
		return b.mergeDML(suffix, filter), nil
	case DMLInsert:
		return b.insertDML(suffix, filter), nil
	case DMLDelete:
		return b.deleteDML(suffix, filter), nil
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

func (b *Builder) baseInsert(suffix string, withReplace bool) string {
	names, values := b.insertNameAndValues()
	replace := ""
	if withReplace {
		replace = "OR REPLACE"
	}
	return fmt.Sprintf("INSERT %v INTO %v(%v) SELECT %v FROM %v t", replace, b.Table(""), names, values, b.Table(suffix))
}


func (b *Builder) insertUpdateDML(suffix string, wfilter map[string]interface{}) string {
	DML := b.baseInsert(suffix, false)
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

func (b *Builder) mergeDML(suffix string, filter map[string]interface{}) (string) {
	DQL := b.DQL("_tmp", b.dest, nil)
	var onCriteria = make([]string, 0)
	for _, column := range b.UniqueColumns {
		onCriteria = append(onCriteria, fmt.Sprintf("d.%v = t.%v", column, column))
	}
	filterKeys := toolbox.MapKeysToStringSlice(filter)
	sort.Strings(filterKeys)
	for _, k := range filterKeys {
		v := filter[k]
		onCriteria = append(onCriteria, toCriterion("d."+k, v))
	}
	setValues := b.updateSetValues()
	names, values := b.insertNameAndValues()
	return fmt.Sprintf(mergeSQL,
		b.Table(suffix),
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
			innerCriteria = append(innerCriteria, toCriterion("t."+k, v))
		}
		if len(innerCriteria) > 0 {
			innerWhere = "WHERE  " + strings.Join(innerCriteria, " AND ")
		}
		inCriteria := fmt.Sprintf("(%v) NOT IN (SELECT %v FROM %v t %v)", strings.Join(b.UniqueColumns, ","), strings.Join(b.UniqueColumns, ","), b.Table(""), innerWhere)
		return "\nWHERE " + inCriteria
	}
	return ""
}

func (b *Builder) insertDML(suffix string, filter map[string]interface{}) string {
	return b.baseInsert(suffix, false) + b.getInsertWhereClause(filter)
}

func (b *Builder) insertReplaceDML(suffix string, filter map[string]interface{}) string {
	return b.baseInsert(suffix, true) + b.getInsertWhereClause(filter)
}

func (b *Builder) deleteDML(suffix string, filter map[string]interface{}) string {
	var whereClause = make([]string, 0)
	filterKeys := toolbox.MapKeysToStringSlice(filter)
	sort.Strings(filterKeys)
	for _, k := range filterKeys {
		v := filter[k]
		whereClause = append(whereClause, toCriterion("d."+k, v))
	}
	if len(b.UniqueColumns) > 0 {
		inCriteria := fmt.Sprintf("(%v) NOT IN (SELECT %v FROM %v)", strings.Join(b.UniqueColumns, ","), strings.Join(b.UniqueColumns, ","), b.Table(suffix))
		whereClause = append(whereClause, inCriteria)
	}
	return fmt.Sprintf("DELETE FROM %v WEHRE  %v", b.Table(""), strings.Join(whereClause, " AND "))
}


func (b *Builder) defaultChunkDQL() string {
	if len(b.UniqueColumns) == 0 {
		return ""
	}
	var projection = []string{
		fmt.Sprintf("MIN(%v) AS min_value", b.UniqueColumns[0]),
		fmt.Sprintf("MAX(%v) AS max_valu", b.UniqueColumns[0]),
		"COUNT(1) AS count_value",
	}
	return fmt.Sprintf(`SELECT %v
FROM %v t 
$whereClause
OFFSET $min LIMIT $limit`, strings.Join(projection, ",\n\t"), b.Table(""))
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
	return builder, nil
}
