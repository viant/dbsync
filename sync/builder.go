package sync

import (
	"dbsync/sync/diff"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"sort"
	"strings"
)

//Builder represents SQL builder
type Builder struct {
	Strategy         //request sync meta
	ddl              string
	taskID           string
	transferSuffix   string
	tempDatabase     string
	uniques          map[string]bool
	source           *Resource
	dest             *Resource
	table            string
	columns          []dsc.Column
	columnsByName    map[string]dsc.Column
	isUpperCase      bool
	maxIDColumnAlias string
	minIDColumnAlias string

	countColumnAlias       string
	uniqueCountAlias       string
	uniqueNotNullSumtAlias string
}

//Table returns table name
func (b *Builder) Table(suffix string) string {
	if suffix == "" {
		return b.table
	}
	suffix = normalizeTableName(suffix)
	if b.tempDatabase != "" {
		return b.tempDatabase + "." + b.table + b.transferSuffix + suffix
	}
	return b.table + b.transferSuffix + suffix
}

//QueryTable returns query table
func (b *Builder) QueryTable(suffix string, resource *Resource) string {
	suffix = normalizeTableName(suffix)
	if suffix == "" {
		if resource.From != "" {
			return fmt.Sprintf("(%s)", resource.From)
		}
		return b.table
	}

	if b.tempDatabase != "" {
		return b.tempDatabase + "." + b.table + b.transferSuffix + suffix
	}
	return b.table + b.transferSuffix + suffix
}

//DDLAsSelect returns transient table DDL for supplied suffix
func (b *Builder) DDLFromSelect(suffix string) string {
	suffix = normalizeTableName(suffix)
	return fmt.Sprintf("CREATE TABLE %v AS SELECT * FROM %v WHERE 1 = 0", b.Table(suffix), b.Table(""))
}

//DDL returns transient table DDL for supplied suffix
func (b *Builder) DDL(tempTable string) string {
	DDL := b.ddl
	if tempTable != "" {
		DDL = strings.Replace(DDL, b.Table(""), b.Table(tempTable), 1)
	}
	DDL = strings.Replace(DDL, ";", "", 1)
	return DDL
}

func (b *Builder) columnExpression(column string, resource *Resource) string {
	if pseudoColumn, ok := resource.columnExpression[column]; ok {
		return pseudoColumn.Expression + " AS " + b.formatColumn(column)
	}
	return column
}

func (b *Builder) unAliasedColumnExpression(column string, resource *Resource) string {
	if pseudoColumn, ok := resource.columnExpression[column]; ok {
		return pseudoColumn.Expression
	}
	return column
}

func (b *Builder) defaultChunkDQL(resource *Resource) string {
	var projection = []string{
		fmt.Sprintf("COUNT(1) AS %v", b.formatColumn("count_value")),
	}
	if len(b.uniques) > 0 {
		projection = append(projection,
			fmt.Sprintf("MIN(%v) AS %v", b.IDColumns[0], b.formatColumn("min_value")),
			fmt.Sprintf("MAX(%v) AS %v", b.IDColumns[0], b.formatColumn("max_value")))
	}
	return fmt.Sprintf(`SELECT %v
FROM (
SELECT %v
FROM %v t $whereClause
ORDER BY %v
LIMIT $limit 
) t`, strings.Join(projection, ",\n\t"), b.IDColumns[0], b.QueryTable("", resource), b.IDColumns[0])
}

//ChunkDQL returns chunk DQL
func (b *Builder) ChunkDQL(resource *Resource, max, limit int, values map[string]interface{}) (string, error) {
	state := data.NewMap()
	state.Put("hint", resource.Hint)
	state.Put("max", max)

	state.Put("limit", limit)
	state.Put("where", "")
	state.Put("whereClause", "")
	if len(values) > 0 {
		var whereCriteria = b.toCriteriaList(values, resource)
		whereCriteria = append(whereCriteria, b.toCriteriaList(resource.Criteria, resource)...)
		state.Put("where", " AND "+strings.Join(whereCriteria, " AND "))
		state.Put("whereClause", " WHERE "+strings.Join(whereCriteria, " AND "))
	}
	chunkSQL := resource.ChunkSQL
	if chunkSQL == "" {
		chunkSQL = b.defaultChunkDQL(resource)
	}
	DQL := state.ExpandAsText(chunkSQL)
	return DQL, nil
}

func (b *Builder) toCriteriaList(criteria map[string]interface{}, resource *Resource) []string {
	var whereCriteria = make([]string, 0)
	if len(criteria) == 0 {
		return whereCriteria
	}

	keys := toolbox.MapKeysToStringSlice(criteria)
	sort.Strings(keys)
	for _, k := range keys {
		v := criteria[k]
		column := b.unAliasedColumnExpression(k, resource)
		whereCriteria = append(whereCriteria, toCriterion(column, v))
	}
	return whereCriteria
}

func (b *Builder) toWhereCriteria(criteria map[string]interface{}, resource *Resource) string {
	if len(criteria) == 0 && len(resource.Criteria) == 0 {
		return ""
	}
	criteriaList := b.toCriteriaList(criteria, resource)
	criteriaList = append(criteriaList, b.toCriteriaList(resource.Criteria, resource)...)
	return "\nWHERE " + strings.Join(criteriaList, " AND ")
}

//CountDQL returns count DQL for supplied resource and filter
func (b *Builder) CountDQL(suffix string, resource *Resource, criteria map[string]interface{}) string {
	var projection = []string{
		fmt.Sprintf("COUNT(1) AS %v", b.formatColumn("count_value")),
	}

	if len(b.uniques) == 1 {
		projection = append(projection,
			fmt.Sprintf("MIN(%v) AS %v", b.IDColumns[0], b.formatColumn("min_value")),
			fmt.Sprintf("MAX(%v) AS %v", b.IDColumns[0], b.formatColumn("max_value")))
	}

	DQL := fmt.Sprintf("SELECT %v\nFROM %v t %v",
		strings.Join(projection, ",\n\t"),
		b.QueryTable(suffix, resource),
		b.toWhereCriteria(criteria, resource))
	return DQL
}

func (b *Builder) isUnique(candidate string) bool {
	_, has := b.uniques[strings.ToLower(candidate)]
	return has
}

//DQL returns sync DQL
func (b *Builder) DQL(suffix string, resource *Resource, values map[string]interface{}, dedupe bool) string {
	var projection = make([]string, 0)
	var dedupeFunction = ""
	if dedupe {
		dedupeFunction = "MAX"
	}

	for _, column := range b.columns {
		if b.isUnique(column.Name()) {
			continue
		}
		if dedupe {
			expression, ok := resource.columnExpression[column.Name()]
			if !ok {
				projection = append(projection, fmt.Sprintf("%v(%v) AS %v", dedupeFunction, column.Name(), b.formatColumn(column.Name())))
				continue
			}
			projection = append(projection, fmt.Sprintf("%v(%v) AS %v", dedupeFunction, expression.Expression, b.formatColumn(column.Name())))
			continue
		}
		expression, ok := resource.columnExpression[column.Name()]
		if !ok {
			projection = append(projection, fmt.Sprintf("%v", b.formatColumn(column.Name())))
			continue
		}
		projection = append(projection, fmt.Sprintf("%v AS %v", expression.Expression, b.formatColumn(column.Name())))
	}

	if len(b.IDColumns) > 0 {
		projection = append(b.IDColumns, projection...)
	}
	DQL := fmt.Sprintf("SELECT %v %v\nFROM %v t ", resource.Hint, strings.Join(projection, ",\n"), b.QueryTable(suffix, resource))

	if len(values) > 0 || len(resource.Criteria) > 0 {
		whereCriteria := b.toCriteriaList(values, resource)
		whereCriteria = append(whereCriteria, b.toCriteriaList(resource.Criteria, resource)...)
		DQL += "\nWHERE " + strings.Join(whereCriteria, " AND ")
	}
	if dedupeFunction != "" {
		DQL += fmt.Sprintf("\nGROUP BY %v", strings.Join(b.IDColumns, ","))
	}
	return DQL
}

func (b *Builder) init() {
	b.uniques = make(map[string]bool)
	if len(b.IDColumns) == 0 {
		b.IDColumns = make([]string, 0)
	}
	b.source.indexPseudoColumns()
	b.dest.indexPseudoColumns()

	for _, column := range b.columns {
		b.columnsByName[column.Name()] = column
	}

	if len(b.Diff.Columns) == 0 {
		if b.Diff.CountOnly {
			b.Diff.Columns = b.buildDiffColumns(nil)
		} else {
			b.Diff.Columns = b.buildDiffColumns(b.columns)
		}
	} else {

		for _, diffColumn := range b.Diff.Columns {
			if diffColumn.DateLayout == "" && diffColumn.DateFormat != "" {
				diffColumn.DateLayout = toolbox.DateFormatToLayout(diffColumn.DateFormat)
			}
			if diffColumn.Alias == "" {
				diffColumn.Alias = b.formatColumn(diffColumn.Func + "_" + diffColumn.Name)
			}
		}
	}
	b.addStandardDiffColumns()
	for _, column := range b.IDColumns {
		b.uniques[strings.ToLower(column)] = true
	}
}

//DiffDQL returns sync difference DQL
func (b *Builder) DiffDQL(criteria map[string]interface{}, resource *Resource) (string, []string) {
	return b.partitionDQL(criteria, resource, func(projection *[]string, dimension map[string]bool) {
		for _, column := range b.Diff.Columns {
			if _, has := dimension[column.Name]; has {
				continue
			}
			*projection = append(*projection, b.formatColumn(column.Expr(resource.columnExpr)))
		}
	})
}

func (b *Builder) partitionDQL(criteria map[string]interface{}, resource *Resource, projectionGenerator func(projection *[]string, dimension map[string]bool)) (string, []string) {
	var projection = make([]string, 0)
	var groupBy = make([]string, 0)
	var i = 1
	var dimension = make(map[string]bool)
	for _, partition := range b.Partition.Columns {
		if _, has := dimension[partition]; has {
			continue
		}
		dimension[partition] = true
		aliasedExpression := b.columnExpression(partition, resource)
		projection = append(projection, aliasedExpression)
		if resource.PositionReference {
			groupBy = append(groupBy, fmt.Sprintf("%d", i))
			i++
		} else {
			rawExpression := b.unAliasedColumnExpression(partition, resource)
			groupBy = append(groupBy, rawExpression)
		}
	}

	projectionGenerator(&projection, dimension)
	SQL := fmt.Sprintf("SELECT %v\nFROM %s t", strings.Join(projection, ",\n\t"), b.QueryTable("", resource))
	SQL += b.toWhereCriteria(criteria, resource)
	if len(groupBy) > 0 {
		SQL += fmt.Sprintf("\nGROUP BY %s", strings.Join(groupBy, ","))
	}
	if len(groupBy) > 0 {
		SQL += fmt.Sprintf("\nORDER BY %s", strings.Join(groupBy, ","))
	}
	return SQL, toolbox.MapKeysToStringSlice(dimension)
}

//DML returns DML
func (b *Builder) DML(dmlType string, suffix string, filter map[string]interface{}) (string, error) {
	if suffix == "" {
		return "", fmt.Errorf("sufifx was empty")
	}
	switch dmlType {
	case DMLInsertOrReplace:
		return b.insertReplaceDML(suffix, filter), nil
	case DMLInsertOnDuplicateUpddate:
		return b.insertOnDuplicateUpdateDML(suffix, filter), nil
	case DMLInsertOnConflictUpddate:
		return b.insertOnConflictUpdateDML(suffix, filter), nil
	case DMLMerge:
		return b.mergeDML(suffix, filter), nil
	case DMLMergeInto:
		return b.mergeIntoDML(suffix, filter), nil
	case DMLInsert:
		return b.insertDML(suffix, filter), nil
	case transientDMLDelete:
		return b.transientDeleteDML(suffix, filter), nil
	case DMLDelete:
		return b.deleteDML(suffix, filter), nil
	}
	return "", fmt.Errorf("unsupported %v", dmlType)
}

func (b *Builder) insertNameAndValues() (string, string) {
	return b.aliasedInsertNameAndValues("", "")
}

func (b *Builder) aliasValue(alias string, value string, resource *Resource) string {
	if alias == "" {
		return b.columnExpression(value, resource)
	}
	expression := b.columnExpression(value, resource)
	if expression == value {
		value = alias + value
	} else {
		value = strings.Replace(expression, "t.", alias, strings.Count(expression, "t."))
	}
	return value
}

func (b *Builder) aliasedInsertNameAndValues(srcAlias, destAlias string) (string, string) {
	var names = make([]string, 0)
	var values = make([]string, 0)

	for _, column := range b.IDColumns {
		names = append(names, destAlias+column)
		values = append(values, b.aliasValue(srcAlias, column, b.dest))
	}
	for _, column := range b.columns {
		if b.isUnique(column.Name()) {
			continue
		}
		names = append(names, destAlias+column.Name())
		values = append(values, b.aliasValue(srcAlias, column.Name(), b.dest))
	}
	return strings.Join(names, ","), strings.Join(values, ",")
}

func (b *Builder) updateSetValues() string {
	update := make([]string, 0)
	for _, column := range b.columns {
		if b.isUnique(column.Name()) {
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
	dedupe := len(b.uniques) > 0
	DQL := b.DQL(suffix, b.dest, nil, dedupe)
	names, values := b.insertNameAndValues()
	replace := ""
	if withReplace {
		replace = "OR REPLACE"
	}
	return fmt.Sprintf("INSERT %v INTO %v(%v) SELECT %v FROM (%v) t", replace, b.Table(""), names, values, DQL)
}

//AppendDML returns append DML
func (b *Builder) AppendDML(sourceSuffix, destSuffix string) string {
	dedupe := len(b.uniques) > 0
	DQL := b.DQL(sourceSuffix, b.dest, nil, dedupe)
	names, values := b.insertNameAndValues()
	return fmt.Sprintf("INSERT INTO %v(%v) SELECT %v FROM (%v) t", b.Table(destSuffix), names, values, DQL)
}

func (b *Builder) insertOnDuplicateUpdateDML(suffix string, wfilter map[string]interface{}) string {
	DML := b.baseInsert(suffix, false)
	return fmt.Sprintf("%v \nON DUPLICATE KEY \n UPDATE %v", DML, b.updateSetValues())
}

func (b *Builder) insertOnConflictUpdateDML(suffix string, filter map[string]interface{}) string {
	DML := b.baseInsert(suffix, false)
	updateDML := b.updateSetValues()
	updateDML = strings.Replace(updateDML, "t.", "excluded.", strings.Count(updateDML, "t."))
	return fmt.Sprintf("%v \nON CONFLICT(%v) DO\n UPDATE SET %v", DML, strings.Join(b.IDColumns, ","), updateDML)
}

const mergeSQL = `
MERGE %v d
USING %v t
ON %v
WHEN MATCHED THEN
  UPDATE SET %v
WHEN NOT MATCHED THEN
  INSERT (%v)
  VALUES(%v)
`

const mergeINTOSQL = `
MERGE INTO %v d
USING %v t
ON (%v)
WHEN MATCHED THEN
  UPDATE SET %v
WHEN NOT MATCHED THEN
  INSERT (%v)
  VALUES(%v)
`

func (b *Builder) filterCriteriaColumn(key, alias string, resource *Resource) string {
	column := b.unAliasedColumnExpression(key, b.dest)
	if strings.Contains(column, ".") && !strings.Contains(column, alias+".") {
		column = strings.Replace(column, "t.", alias+".", len(column))
	} else {
		column = "d." + column
	}
	return column
}

func (b *Builder) mergeIntoDML(suffix string, filter map[string]interface{}) string {
	return b.mergeWithTemplateDML(suffix, filter, mergeINTOSQL, true, true)
}

func (b *Builder) mergeDML(suffix string, filter map[string]interface{}) string {
	return b.mergeWithTemplateDML(suffix, filter, mergeSQL, false, false)
}

func (b *Builder) mergeWithTemplateDML(suffix string, filter map[string]interface{}, template string, idCriteriaOnly bool, insertAlias bool) string {
	dedupe := len(b.uniques) > 0
	DQL := b.DQL(suffix, b.dest, nil, dedupe)
	var onCriteria = make([]string, 0)

	for _, column := range b.IDColumns {
		onCriteria = append(onCriteria, fmt.Sprintf("d.%v = t.%v", column, column))
	}
	filterKeys := toolbox.MapKeysToStringSlice(filter)
	sort.Strings(filterKeys)
	if !idCriteriaOnly {
		for _, k := range filterKeys {
			v := filter[k]
			column := b.filterCriteriaColumn(k, "d", b.dest)
			onCriteria = append(onCriteria, toCriterion(column, v))
		}
	}
	setValues := b.updateSetValues()
	var srcAlias, destAlias string
	if insertAlias {
		srcAlias = "t."
		destAlias = "d."
	}
	names, values := b.aliasedInsertNameAndValues(srcAlias, destAlias)
	return fmt.Sprintf(template,
		b.Table(""),
		fmt.Sprintf("(%v)", DQL),
		strings.Join(onCriteria, " AND "),
		setValues,
		names, values)
}

func (b *Builder) getInsertWhereClause(filter map[string]interface{}) string {
	if len(b.IDColumns) > 0 {
		innerWhere := ""
		var innerCriteria = make([]string, 0)
		filterKeys := toolbox.MapKeysToStringSlice(filter)
		sort.Strings(filterKeys)
		for _, k := range filterKeys {
			v := filter[k]
			column := b.filterCriteriaColumn(k, "t", b.dest)
			innerCriteria = append(innerCriteria, toCriterion(column, v))
		}
		if len(innerCriteria) > 0 {
			innerWhere = "WHERE  " + strings.Join(innerCriteria, " AND ")
		}
		inCriteria := fmt.Sprintf("(%v) NOT IN (SELECT %v FROM %v t %v)", strings.Join(b.IDColumns, ","), strings.Join(b.IDColumns, ","), b.Table(""), innerWhere)
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

func (b *Builder) transientDeleteDML(suffix string, filter map[string]interface{}) string {
	whereClause := b.toWhereCriteria(filter, b.dest)
	if len(b.IDColumns) > 0 {
		uniqueExpr := strings.Join(b.IDColumns, ",")
		inCriteria := fmt.Sprintf("(%v) IN (SELECT %v FROM %v t %v)", uniqueExpr, uniqueExpr, b.Table(""), whereClause)
		if whereClause != "" {
			whereClause += " AND " + inCriteria
		} else {
			whereClause = " WHERE " + inCriteria
		}
	}
	whereClause = removeTableAliases(whereClause, "t")
	return fmt.Sprintf("DELETE FROM %v %v", b.Table(suffix), whereClause)
}

func (b *Builder) deleteDML(suffix string, filter map[string]interface{}) string {
	whereClause := b.toWhereCriteria(filter, b.dest)
	if len(b.IDColumns) > 0 {
		uniqueExpr := strings.Join(b.IDColumns, ",")
		inCriteria := fmt.Sprintf("(%v) NOT IN (SELECT %v FROM %v)", uniqueExpr, uniqueExpr, b.Table(suffix))
		if whereClause != "" {
			whereClause += " AND " + inCriteria
		} else {
			whereClause = " WHERE " + inCriteria
		}
	}
	whereClause = removeTableAliases(whereClause, "t")
	return fmt.Sprintf("DELETE FROM %v %v", b.Table(""), whereClause)
}

func (b *Builder) formatColumn(column string) string {
	if b.isUpperCase {
		return strings.ToUpper(column)
	}
	return column
}

func (b *Builder) addStandardDiffColumns() {
	b.countColumnAlias = b.formatColumn("cnt")
	for _, candidate := range b.Diff.Columns {
		if candidate.Name == b.countColumnAlias {
			return
		}
	}
	b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
		Func:  "COUNT",
		Name:  "1",
		Alias: b.formatColumn("cnt"),
	})

	for _, unique := range b.IDColumns {
		if len(b.IDColumns) == 1 {
			b.maxIDColumnAlias = b.formatColumn("max_" + unique)
			b.minIDColumnAlias = b.formatColumn("min_" + unique)

			b.uniqueCountAlias = b.formatColumn("unique_cnt")
			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "COUNT",
				Name:  unique,
				Alias: b.uniqueCountAlias,
			})
			b.uniqueNotNullSumtAlias = b.formatColumn("non_cnt")
			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "SUM",
				Name:  "(CASE WHEN " + unique + " IS NOT NULL THEN 1 ELSE 0 END)",
				Alias: b.uniqueNotNullSumtAlias,
			})
			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "MAX",
				Name:  unique,
				Alias: b.maxIDColumnAlias,
			})
			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "MIN",
				Name:  unique,
				Alias: b.minIDColumnAlias,
			})

		}

	}
}

func (b *Builder) buildDiffColumns(columns []dsc.Column) []*diff.Column {
	var result = make([]*diff.Column, 0)

	if len(columns) == 0 {
		return result
	}
	for _, column := range columns {

		if b.isUnique(column.Name()) {
			continue
		}
		diffColumn := &diff.Column{
			Name: column.Name(),
		}
		prefix := ""
		switch strings.ToUpper(column.DatabaseTypeName()) {
		case "BOOL", "BOOLEAN":
			diffColumn.Func = "COUNT"
			diffColumn.Default = false
			prefix = "cnt_"
		case "TINYINT", "BIT":
			diffColumn.Func = "COUNT"
			diffColumn.Default = 0
			prefix = "cnt_"
		case "FLOAT", "NUMERIC", "DECIMAL", "FLOAT64", "INTEGER", "INT", "SMALLINT", "BIGINT":
			diffColumn.Func = "SUM"
			prefix = "sum_"
			diffColumn.Default = 0
			if diffColumn.NumericPrecision == 0 {
				diffColumn.NumericPrecision = b.Diff.NumericPrecision
			}
		case "TIMESTAMP", "TIME", "DATE", "DATETIME":
			diffColumn.Func = "MAX"
			prefix = "max_"
			if diffColumn.DateLayout == "" {
				diffColumn.DateLayout = b.Diff.DateLayout
			}

		default:
			diffColumn.Func = "COUNT"
			diffColumn.Default = " "
			prefix = "cnt_"
		}
		diffColumn.Alias = b.formatColumn(prefix + diffColumn.Name)
		result = append(result, diffColumn)
	}
	return result
}

//NewBuilder creates a new builder
func NewBuilder(request *Request, ddl string, isUpperCaseTable bool, destColumns []dsc.Column) (*Builder, error) {
	transferSuffix := request.Transfer.Suffix
	if transferSuffix != "" && !strings.HasPrefix(transferSuffix, "_") {
		transferSuffix = "_" + transferSuffix
	}
	builder := &Builder{
		tempDatabase:   request.Transfer.TempDatabase,
		Strategy:       request.Strategy,
		columns:        destColumns,
		columnsByName:  make(map[string]dsc.Column),
		table:          request.Dest.Table,
		source:         request.Source,
		ddl:            ddl,
		dest:           request.Dest,
		isUpperCase:    isUpperCaseTable,
		transferSuffix: transferSuffix,
		taskID:         request.ID(),
	}
	builder.init()
	if isUpperCaseTable {
		request.UseUpperCase()
	}
	return builder, nil
}
