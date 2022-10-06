package sql

import (
	"dbsync/sync/contract"
	"dbsync/sync/criteria"

	"dbsync/sync/contract/strategy"
	"dbsync/sync/contract/strategy/diff"
	"dbsync/sync/shared"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"sort"
	"strings"
)

var reservedKeyword = map[string]bool{
	"window": true,
}

//Builder represents SQL builder
type Builder struct {
	*strategy.Strategy //request sync meta
	ddl                string
	transferSuffix     string
	tempDatabase       string
	uniques            map[string]bool
	source             *contract.Resource
	dest               *contract.Resource
	table              string
	columns            []dsc.Column
	columnsByName      map[string]dsc.Column
	useCrateLikeDDL    bool
	isUpperCase        bool
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
func (b *Builder) QueryTable(suffix string, resource *contract.Resource) string {
	suffix = normalizeTableName(suffix)
	if suffix == "" {
		if resource.From != "" {
			return fmt.Sprintf("(%s)", resource.From)
		}
		return resource.Table
	}
	if b.tempDatabase != "" {
		return b.tempDatabase + "." + b.table + b.transferSuffix + suffix
	}
	return resource.Table + b.transferSuffix + suffix
}

//DDLFromSelect returns transient table DDL for supplied suffix
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

func (b *Builder) columnExpression(column string, resource *contract.Resource) string {
	if pseudoColumn := resource.GetPseudoColumn(column); pseudoColumn != nil {
		return pseudoColumn.Expression + " AS " + b.formatColumn(column)
	}
	return column
}

func (b *Builder) unAliasedColumnExpression(column string, resource *contract.Resource) string {
	if pseudoColumn := resource.GetPseudoColumn(column); pseudoColumn != nil {
		return pseudoColumn.Expression
	}
	if reservedKeyword[column] {
		return fmt.Sprintf("`%v`", column)
	}
	return column
}

func (b *Builder) defaultChunkDQL(resource *contract.Resource) string {
	var projection = []string{
		fmt.Sprintf("COUNT(1) AS %v", b.formatColumn("count_value")),
	}
	if len(b.uniques) > 0 {
		projection = append(projection,
			fmt.Sprintf("MIN(%v) AS %v", b.unAliasedColumnExpression(b.IDColumns[0], resource), b.formatColumn("min_value")),
			fmt.Sprintf("MAX(%v) AS %v", b.unAliasedColumnExpression(b.IDColumns[0], resource), b.formatColumn("max_value")))
	}
	return fmt.Sprintf(`SELECT %v
FROM (
SELECT %v
FROM %v t $whereClause
ORDER BY %v
LIMIT $limit 
) t`, strings.Join(projection, ",\n\t"), b.unAliasedColumnExpression(b.IDColumns[0], resource), b.QueryTable("", resource), b.IDColumns[0])
}

//ChunkDQL returns chunk DQL
func (b *Builder) ChunkDQL(resource *contract.Resource, max, limit int, values map[string]interface{}) string {
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
	return DQL
}

func (b *Builder) toCriteriaList(filter map[string]interface{}, resource *contract.Resource) []string {
	var whereCriteria = make([]string, 0)
	if len(filter) == 0 {
		return whereCriteria
	}

	keys := toolbox.MapKeysToStringSlice(filter)
	sort.Strings(keys)
	for _, k := range keys {
		v := filter[k]
		column := b.unAliasedColumnExpression(k, resource)
		whereCriteria = append(whereCriteria, criteria.ToCriterion(column, v))
	}
	return whereCriteria
}

func (b *Builder) toWhereCriteria(criteria map[string]interface{}, resource *contract.Resource) string {
	if len(criteria) == 0 && len(resource.Criteria) == 0 {
		return ""
	}
	criteriaList := b.toCriteriaList(criteria, resource)
	criteriaList = append(criteriaList, b.toCriteriaList(resource.Criteria, resource)...)
	return "\nWHERE " + strings.Join(criteriaList, " AND ")
}

//CountDQL returns count DQL for supplied resource and filter
func (b *Builder) CountDQL(suffix string, resource *contract.Resource, criteria map[string]interface{}) string {
	var projection = []string{
		fmt.Sprintf("COUNT(1) AS %v", b.formatColumn("count_value")),
	}

	if len(b.uniques) == 1 {
		projection = append(projection,
			fmt.Sprintf("MIN(%v) AS %v", b.unAliasedColumnExpression(b.IDColumns[0], resource), b.formatColumn("min_value")),
			fmt.Sprintf("MAX(%v) AS %v", b.unAliasedColumnExpression(b.IDColumns[0], resource), b.formatColumn("max_value")))
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
func (b *Builder) DQL(suffix string, resource *contract.Resource, values map[string]interface{}, dedupe bool) string {
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
			expression := resource.GetPseudoColumn(column.Name())
			if expression == nil {
				projection = append(projection, fmt.Sprintf("%v(%v) AS %v", dedupeFunction, column.Name(), b.formatColumn(column.Name())))
				continue
			}
			projection = append(projection, fmt.Sprintf("%v(%v) AS %v", dedupeFunction, expression.Expression, b.formatColumn(column.Name())))
			continue
		}
		expression := resource.GetPseudoColumn(column.Name())
		if expression == nil {
			projection = append(projection, fmt.Sprintf("%v", b.formatColumn(column.Name())))
			continue
		}
		projection = append(projection, fmt.Sprintf("%v AS %v", expression.Expression, b.formatColumn(column.Name())))
	}

	idColumnProjection := make([]string, 0)
	if len(b.IDColumns) > 0 {
		for i := range b.IDColumns {
			idColumnProjection = append(idColumnProjection, b.columnExpression(b.IDColumns[i], resource))
		}
		projection = append(idColumnProjection, projection...)
	}

	DQL := fmt.Sprintf("SELECT %v %v\nFROM %v t ", resource.Hint, strings.Join(projection, ",\n"), b.QueryTable(suffix, resource))

	if len(values) > 0 || len(resource.Criteria) > 0 {
		whereCriteria := b.toCriteriaList(values, resource)
		whereCriteria = append(whereCriteria, b.toCriteriaList(resource.Criteria, resource)...)
		DQL += "\nWHERE " + strings.Join(whereCriteria, " AND ")
	}
	if dedupeFunction != "" {
		DQL += fmt.Sprintf("\nGROUP BY %v", strings.Join(idColumnProjection, ","))
	}
	return DQL
}

func (b *Builder) init() {

	b.uniques = make(map[string]bool)
	if len(b.IDColumns) == 0 {
		b.IDColumns = make([]string, 0)
	}
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
	b.addStandardSignatureColumns()
	for _, column := range b.IDColumns {
		b.uniques[strings.ToLower(column)] = true
	}
}

//SignatureDQL returns sync difference DQL
func (b *Builder) SignatureDQL(resource *contract.Resource, criteria map[string]interface{}) string {
	return b.partitionDQL(criteria, resource, func(projection *[]string, dimension map[string]bool) {
		for _, column := range b.Diff.Columns {
			if _, has := dimension[column.Name]; has {
				continue
			}
			*projection = append(*projection, b.formatColumn(column.Expr(resource.BaseColumnExpr)))
		}
	})
}

func (b *Builder) partitionDQL(criteria map[string]interface{}, resource *contract.Resource, projectionGenerator func(projection *[]string, dimension map[string]bool)) string {
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
	return SQL
}

//DML returns DML
func (b *Builder) DML(dmlType string, suffix string, filter map[string]interface{}) (string, error) {
	if suffix == "" && dmlType != shared.DMLFilteredDelete {
		return "", fmt.Errorf("sufifx was empty")
	}
	switch dmlType {
	case shared.DMLInsertOrReplace:
		return b.insertReplaceDML(suffix, filter), nil
	case shared.DMLInsertOnDuplicateUpddate:
		return b.insertOnDuplicateUpdateDML(suffix, filter), nil
	case shared.DMLInsertOnConflictUpddate:
		return b.insertOnConflictUpdateDML(suffix, filter), nil
	case shared.DMLMerge:
		return b.mergeDML(suffix, filter), nil
	case shared.DMLMergeInto:
		return b.mergeIntoDML(suffix, filter), nil
	case shared.DMLInsert:
		return b.insertDML(suffix, filter), nil
	case shared.TransientDMLDelete:
		return b.transientDeleteDML(suffix, filter), nil
	case shared.DMLFilteredDelete:
		if len(filter) == 0 {
			return "", fmt.Errorf("filter was empty")
		}
		return b.deleteWithFilterDML(suffix, filter), nil
	case shared.DMLDelete:
		return b.deleteDML(suffix, filter), nil
	}
	return "", fmt.Errorf("unsupported %v", dmlType)
}

func (b *Builder) insertNameAndValues() (string, string) {
	return b.aliasedInsertNameAndValues("", "")
}

func (b *Builder) aliasValue(alias string, value string, resource *contract.Resource) string {
	if alias == "" {
		return b.unAliasedColumnExpression(value, resource)
	}
	expression := b.unAliasedColumnExpression(value, resource)
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
		columnName := column.Name()
		if reservedKeyword[column.Name()] {
			columnName = fmt.Sprintf("`%v`", columnName)
		}

		names = append(names, destAlias+columnName)
		values = append(values, b.aliasValue(srcAlias, columnName, b.dest))
	}
	return strings.Join(names, ","), strings.Join(values, ",")
}

func (b *Builder) updateSetValues(destAlias string) string {
	update := make([]string, 0)
	for _, column := range b.columns {
		if b.isUnique(column.Name()) {
			continue
		}
		value := b.unAliasedColumnExpression(column.Name(), b.dest)
		if value == column.Name() {
			value = "t." + column.Name()
		}
		update = append(update, fmt.Sprintf("%v%v = %v", destAlias, column.Name(), value))
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
	return fmt.Sprintf("%v \nON DUPLICATE KEY \n UPDATE %v", DML, b.updateSetValues(""))
}

func (b *Builder) insertOnConflictUpdateDML(suffix string, filter map[string]interface{}) string {
	DML := b.baseInsert(suffix, false)
	updateDML := b.updateSetValues("")
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

func (b *Builder) filterCriteriaColumn(key, alias string, resource *contract.Resource) string {
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
			onCriteria = append(onCriteria, criteria.ToCriterion(column, v))
		}
	}
	setValues := b.updateSetValues("d.")
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

func (b *Builder) getInsertWhereClause(filter map[string]interface{}, dedupe bool) string {
	where := b.getInsertWhere(filter, dedupe)
	if where == "" {
		return ""
	}
	aliasCount := strings.Count(where, "d.")
	if aliasCount == 0 {
		return where
	}
	return strings.Replace(where, "d.", "", aliasCount)
}

func (b *Builder) getInsertWhere(filter map[string]interface{}, dedupe bool) string {
	if len(b.IDColumns) > 0 {
		innerWhere := ""
		var innerCriteria = make([]string, 0)
		filterKeys := toolbox.MapKeysToStringSlice(filter)
		sort.Strings(filterKeys)
		for _, k := range filterKeys {
			v := filter[k]
			column := b.filterCriteriaColumn(k, "t", b.dest)
			innerCriteria = append(innerCriteria, criteria.ToCriterion(column, v))
		}
		if len(innerCriteria) > 0 {
			innerWhere = "\nWHERE  " + strings.Join(innerCriteria, " AND ")
		}
		if !dedupe {
			return innerWhere
		}
		inCriteria := fmt.Sprintf("(%v) NOT IN (SELECT %v FROM %v t %v)", strings.Join(b.IDColumns, ","), strings.Join(b.IDColumns, ","), b.Table(""), innerWhere)
		return "\nWHERE " + inCriteria
	}
	return ""
}

func (b *Builder) insertDML(suffix string, filter map[string]interface{}) string {
	return b.baseInsert(suffix, false) + b.getInsertWhereClause(filter, true)
}

func (b *Builder) insertReplaceDML(suffix string, filter map[string]interface{}) string {
	return b.baseInsert(suffix, true) + b.getInsertWhereClause(filter, false)
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

func (b *Builder) deleteWithFilterDML(suffix string, filter map[string]interface{}) string {
	whereClause := b.toWhereCriteria(filter, b.dest)
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
	if reservedKeyword[column] {
		return fmt.Sprintf("`%v`", column)
	}
	if b.isUpperCase {
		column = strings.ToUpper(column)
		aliasedCount := strings.Count(column, "T.")
		if aliasedCount > 0 {
			column = strings.Replace(column, "T.", "t.", aliasedCount)
		}
	}
	return column
}

func (b *Builder) hasDefinedCount() bool {
	countColumnAlias := diff.AliasCount
	for _, candidate := range b.Diff.Columns {
		if strings.ToLower(candidate.Alias) == countColumnAlias {
			return true
		}
	}
	return false
}

func (b *Builder) addStandardSignatureColumns() {

	if b.hasDefinedCount() {
		return
	}

	b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
		Func:  "COUNT",
		Name:  "1",
		Alias: b.formatColumn("cnt"),
	})

	for _, unique := range b.IDColumns {
		if len(b.IDColumns) == 1 {
			idKey := b.IDColumns[0]
			minColumnAlias := fmt.Sprintf(diff.AliasMinIdTemplate, idKey)
			maxColumnAlias := fmt.Sprintf(diff.AliasMaxIdTemplate, idKey)
			uniqueIDCountColumnAlias := fmt.Sprintf(diff.AliasUniqueIDCountTemplate, idKey)
			nonNullIDCountColumnAlias := fmt.Sprintf(diff.AliasNonNullIDCountTemplate, idKey)

			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "COUNT",
				Name:  unique,
				Alias: b.formatColumn(uniqueIDCountColumnAlias),
			})

			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "SUM",
				Base:  unique,
				Name:  "(CASE WHEN " + unique + " IS NOT NULL THEN 1 ELSE 0 END)",
				Alias: b.formatColumn(nonNullIDCountColumnAlias),
			})
			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "MAX",
				Name:  unique,
				Alias: b.formatColumn(maxColumnAlias),
			})
			b.Diff.Columns = append(b.Diff.Columns, &diff.Column{
				Func:  "MIN",
				Name:  unique,
				Alias: b.formatColumn(minColumnAlias),
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

func isUpperCaseTable(columns []dsc.Column) bool {
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		if strings.ToLower(column.Name()) == column.Name() {
			return false
		}
	}
	return true
}

//NewBuilder creates a new builder
func NewBuilder(sync *contract.Sync, ddl string, destColumns []dsc.Column) (*Builder, error) {
	if len(destColumns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	destColumns = filterColumns(sync.Columns, destColumns)
	transferSuffix := sync.Transfer.Suffix
	if transferSuffix != "" && !strings.HasPrefix(transferSuffix, "_") {
		transferSuffix = "_" + transferSuffix
	}
	isUpperCaseTable := isUpperCaseTable(destColumns)
	builder := &Builder{
		tempDatabase:    sync.Transfer.TempDatabase,
		Strategy:        &sync.Strategy,
		columns:         destColumns,
		columnsByName:   make(map[string]dsc.Column),
		table:           sync.Dest.Table,
		source:          sync.Source,
		ddl:             ddl,
		dest:            sync.Dest,
		isUpperCase:     isUpperCaseTable,
		useCrateLikeDDL: sync.UseCreateLikeDDL,
		transferSuffix:  transferSuffix,
	}
	builder.init()

	if isUpperCaseTable {
		sync.UseUpperCaseSQL()
	}
	return builder, nil
}
