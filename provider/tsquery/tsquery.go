package tsquery

import (
	"fmt"
	"sort"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
)

type joinType string

const (
	joinLeft joinType = "LEFT JOIN"
	joinAsOf joinType = "ASOF JOIN"
)

type BuilderParams struct {
	LabelColumns           metadata.ResourceVariantColumns
	SanitizedLabelTable    string
	FeatureColumns         []metadata.ResourceVariantColumns
	SanitizedFeatureTables []string
	FeatureNameVariants    []metadata.ResourceID
}

// NewTrainingSet creates a new training set query builder based on the label and feature columns provided.
func NewTrainingSet(params BuilderParams) *TrainingSet {
	labelTable := labelTable{
		Entity:             params.LabelColumns.Entity,
		Value:              params.LabelColumns.Value,
		TS:                 params.LabelColumns.TS,
		SanitizedTableName: params.SanitizedLabelTable,
	}

	featureTables := make([]featureTable, len(params.FeatureColumns))
	for i, cols := range params.FeatureColumns {
		featureTables[i] = featureTable{
			Entity:             cols.Entity,
			Values:             []string{cols.Value},
			TS:                 cols.TS,
			SanitizedTableName: params.SanitizedFeatureTables[i],
			ColumnAliases:      []string{fmt.Sprintf("feature__%s__%s", params.FeatureNameVariants[i].Name, params.FeatureNameVariants[i].Variant)},
		}
	}

	return &TrainingSet{
		labelTable:    labelTable,
		featureTables: featureTables,
	}
}

// TrainingSet represents a training set query builder.
type TrainingSet struct {
	labelTable    labelTable
	featureTables []featureTable
}

type featureTable struct {
	Entity             string
	Values             []string
	TS                 string
	SanitizedTableName string
	ColumnAliases      []string
}

type labelTable struct {
	Entity             string
	Value              string
	TS                 string
	SanitizedTableName string
}

type featureTableMap map[string]*featureTable

func (ftm featureTableMap) Keys() []string {
	keys := make([]string, len(ftm))
	i := 0
	for k := range ftm {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

// CompileSQL compiles the label and feature tables into a single SQL query
// that uses LEFT JOINs, ASOF JOINs and/or CTEs to create a training set
// based on the presence or absence of timestamps in the label and feature tables.
func (t TrainingSet) CompileSQL() (string, error) {
	if t.labelTable.TS != "" {
		builder := &pitTrainingSetQueryBuilder{labelTable: t.labelTable, featureTableMap: make(map[string]*featureTable)}
		for _, ft := range t.featureTables {
			builder.AddFeature(ft)
		}
		if err := builder.Compile(); err != nil {
			return "", err
		}
		return builder.ToSQL(), nil
	}
	builder := &trainingSetQueryBuilder{labelTable: t.labelTable, featureTableMap: make(map[string]*featureTable)}
	for _, ft := range t.featureTables {
		builder.AddFeature(ft)
	}
	if err := builder.Compile(); err != nil {
		return "", err
	}
	return builder.ToSQL(), nil

}

// ctes represents a list of common table expressions.
type ctes []cte

// ToSQL returns the SQL representation of the CTEs, including the WITH keyword
// and the comma-separated list of CTEs.
func (c ctes) ToSQL() string {
	if len(c) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("WITH ")
	ctes := make([]string, len(c))
	for i, cte := range c {
		ctes[i] = cte.ToSQL()
	}
	sb.WriteString(strings.Join(ctes, ", "))
	sb.WriteString(" ")
	return sb.String()
}

// leftJoins represents a list of LEFT JOINs.
type leftJoins []leftJoin

// ToSQL returns the SQL representation of the LEFT JOINs, space-separated.
func (j leftJoins) ToSQL() string {
	joins := make([]string, len(j))
	for i, join := range j {
		joins[i] = join.ToSQL()
	}
	return strings.Join(joins, " ")
}

// cols represents a list of columns.
type cols []col

// ToSQL returns the SQL representation of the columns, comma-separated.
func (c cols) ToSQL() string {
	cols := make([]string, len(c))
	for i, col := range c {
		cols[i] = col.ToSQL()
	}
	return strings.Join(cols, ", ")
}

// asOfJoins represents a list of ASOF JOINs.
type asOfJoins []asOfJoin

// ToSQL returns the SQL representation of the ASOF JOINs, space-separated.
func (j asOfJoins) ToSQL() string {
	joins := make([]string, len(j))
	for i, join := range j {
		joins[i] = join.ToSQL()
	}
	return strings.Join(joins, " ")
}

// cte represents a common table expression for the case when the label
// does not have a timestamp column, but the feature table does.
type cte struct {
	alias string
	ft    *featureTable
}

// ToSQL creates a row number column (rn) partitioned by the entity column and
// ordered by the timestamp column in descending order.
func (c cte) ToSQL() string {
	selectClause := fmt.Sprintf("SELECT %s, %s, %s", c.ft.Entity, strings.Join(c.ft.Values, ", "), c.ft.TS)
	partitionStmt := fmt.Sprintf(",ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) AS rn ", c.ft.Entity, c.ft.TS)
	fromStmt := fmt.Sprintf("FROM %s ", c.ft.SanitizedTableName)
	cte := fmt.Sprintf("%s AS (%s %s %s)", c.alias, selectClause, partitionStmt, fromStmt)
	return cte
}

// leftJoin represents a LEFT JOIN between the label and feature tables.
type leftJoin struct {
	alias     string
	ft        *featureTable
	lblEntity string
}

// ToSQL creates a LEFT JOIN between the label and feature tables or feature CTEs.
func (j leftJoin) ToSQL() string {
	// CTE
	if j.ft.TS != "" {
		return fmt.Sprintf("LEFT JOIN %s ON l.%s = %s.%s AND %s.rn = 1", j.alias, j.lblEntity, j.alias, j.ft.Entity, j.alias)
	}
	return fmt.Sprintf("LEFT JOIN %s %s ON l.%s = %s.%s", j.ft.SanitizedTableName, j.alias, j.lblEntity, j.alias, j.ft.Entity)
}

// asOfJoin represents an ASOF JOIN between the label and feature tables.
type asOfJoin struct {
	alias     string
	ft        *featureTable
	lblEntity string
	lblTS     string
}

// ToSQL creates an ASOF JOIN between the label and feature tables.
func (j asOfJoin) ToSQL() string {
	joinClause := fmt.Sprintf("%s %s %s", joinAsOf, j.ft.SanitizedTableName, j.alias)
	matchCondition := fmt.Sprintf("MATCH_CONDITION(l.%s >= %s.%s)", j.lblTS, j.alias, j.ft.TS)
	onClause := fmt.Sprintf("ON(l.%s = %s.%s)", j.lblEntity, j.alias, j.ft.Entity)
	return fmt.Sprintf("%s %s %s", joinClause, matchCondition, onClause)
}

// col represents a column in the SELECT clause, with a table alias, column name, and column alias.
type col struct {
	tableAlias string
	val        string
	colAlias   string
}

// ToSQL returns the SQL representation of the column, with the table alias, column name, and column alias.
func (c col) ToSQL() string {
	return fmt.Sprintf("%s.%s AS \"%s\"", c.tableAlias, c.val, c.colAlias)
}

// pitTrainingSetQueryBuilder represents a point-in-time training set query builder.
type pitTrainingSetQueryBuilder struct {
	labelTable      labelTable
	featureTableMap featureTableMap
	columns         cols
	leftJoins       leftJoins
	asOfJoins       asOfJoins
}

// addFeature adds a feature table to the point-in-time training set query builder; if a feature uses
// the same table, entity, and timestamp column as another feature, then the values and column aliases
// are combined into a single feature table so that the query uses a single join for all.
func (b *pitTrainingSetQueryBuilder) AddFeature(tbl featureTable) {
	key := createTableKey(tbl.SanitizedTableName, tbl.Entity, tbl.TS)
	existing, exists := b.featureTableMap[key]
	if exists {
		existing.Values = append(existing.Values, tbl.Values...)
		existing.ColumnAliases = append(existing.ColumnAliases, tbl.ColumnAliases...)
	} else {
		b.featureTableMap[key] = &tbl
	}
}

// Compile compiles the point-in-time training set query builder by creating columns, LEFT JOINs, and ASOF JOINs structs.
func (b *pitTrainingSetQueryBuilder) Compile() error {
	for i, k := range b.featureTableMap.Keys() {
		ft := b.featureTableMap[k]
		ftAlias := fmt.Sprintf("f%d", i+1)
		// COLUMNS
		for i, val := range ft.Values {
			if err := validateFeatureTable(*ft); err != nil {
				return err
			}
			b.columns = append(b.columns, col{tableAlias: ftAlias, val: val, colAlias: ft.ColumnAliases[i]})
		}
		// JOINS
		if ft.TS != "" && b.labelTable.TS != "" {
			b.asOfJoins = append(b.asOfJoins, asOfJoin{alias: ftAlias, ft: ft, lblEntity: b.labelTable.Entity, lblTS: b.labelTable.TS})
		} else {
			b.leftJoins = append(b.leftJoins, leftJoin{alias: ftAlias, ft: ft, lblEntity: b.labelTable.Entity})
		}
	}
	return nil
}

// ToSQL returns the SQL representation of the point-in-time training set query builder.
func (b pitTrainingSetQueryBuilder) ToSQL() string {
	var sb strings.Builder
	// SELECT
	sb.WriteString(fmt.Sprintf("SELECT %s, l.%s AS label", b.columns.ToSQL(), b.labelTable.Value))
	// FROM
	sb.WriteString(fmt.Sprintf(" FROM %s l ", b.labelTable.SanitizedTableName))
	// JOIN(s)
	sb.WriteString(b.leftJoins.ToSQL())
	sb.WriteString(" ")
	sb.WriteString(b.asOfJoins.ToSQL())
	sb.WriteString(";")
	return sb.String()
}

// trainingSetQueryBuilder represents a training set query builder for the case when the label
// does not have a timestamp column, and the feature tables may or may not have a timestamp column.
type trainingSetQueryBuilder struct {
	labelTable      labelTable
	featureTableMap featureTableMap
	ctes            ctes
	columns         cols
	joins           leftJoins
}

// AddFeature adds a feature table to the training set query builder; if a feature uses the same table,
// entity, and timestamp column as another feature, then the values and column aliases are combined into
// a single feature table so that the query uses a single join and/or CTE for all.
func (b *trainingSetQueryBuilder) AddFeature(tbl featureTable) {
	key := createTableKey(tbl.SanitizedTableName, tbl.Entity, tbl.TS)
	existing, exists := b.featureTableMap[key]
	if exists {
		existing.Values = append(existing.Values, tbl.Values...)
		existing.ColumnAliases = append(existing.ColumnAliases, tbl.ColumnAliases...)
	} else {
		b.featureTableMap[key] = &tbl
	}
}

// Compile compiles the training set query builder by creating CTEs, columns, and LEFT JOINs structs.
func (b *trainingSetQueryBuilder) Compile() error {
	if err := validateLabelTable(b.labelTable); err != nil {
		return err
	}
	for i, k := range b.featureTableMap.Keys() {
		ft := b.featureTableMap[k]
		if err := validateFeatureTable(*ft); err != nil {
			return err
		}
		ftAlias := fmt.Sprintf("f%d", i+1)
		usesCTE := ft.TS != ""
		// CTE
		if usesCTE {
			ftAlias = fmt.Sprintf("%s_cte", ftAlias)
			b.ctes = append(b.ctes, cte{alias: ftAlias, ft: ft})
		}
		// JOIN
		b.joins = append(b.joins, leftJoin{alias: ftAlias, ft: ft, lblEntity: b.labelTable.Entity})
		// COLUMNS
		for i, val := range ft.Values {
			b.columns = append(b.columns, col{tableAlias: ftAlias, val: val, colAlias: ft.ColumnAliases[i]})
		}
	}
	return nil
}

// ToSQL returns the SQL representation of the training set query builder.
func (b *trainingSetQueryBuilder) ToSQL() string {
	var sb strings.Builder
	// CTE(s)
	sb.WriteString(b.ctes.ToSQL())
	// SELECT
	sb.WriteString(fmt.Sprintf("SELECT %s, l.%s AS label ", b.columns.ToSQL(), b.labelTable.Value))
	// FROM
	sb.WriteString(fmt.Sprintf("FROM %s l ", b.labelTable.SanitizedTableName))
	// JOIN(s)
	sb.WriteString(b.joins.ToSQL())
	sb.WriteString(";")
	return sb.String()
}

// validateFeatureTable validates the feature table.
func validateFeatureTable(ft featureTable) error {
	if ft.Entity == "" {
		logging.GlobalLogger.Errorw("feature entity cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("feature entity cannot be empty: %v", ft)
	}
	if len(ft.Values) == 0 {
		logging.GlobalLogger.Errorw("values cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("values cannot be empty")
	}
	if ft.SanitizedTableName == "" {
		logging.GlobalLogger.Errorw("sanitized table name cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("sanitized table name cannot be empty")
	}
	if len(ft.ColumnAliases) == 0 {
		logging.GlobalLogger.Errorw("column aliases cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("column aliases cannot be empty")
	}
	if len(ft.Values) != len(ft.ColumnAliases) {
		logging.GlobalLogger.Errorw("values and column aliases must be the same length", "feature_table", ft)
		return fferr.NewInternalErrorf("values and column aliases must be the same length")
	}
	return nil
}

// validateLabelTable validates the label table.
func validateLabelTable(lbl labelTable) error {
	if lbl.Entity == "" {
		logging.GlobalLogger.Errorw("label entity cannot be empty", "label_table", lbl)
		return fferr.NewInternalErrorf("label entity cannot be empty: %v", lbl)
	}
	if lbl.Value == "" {
		logging.GlobalLogger.Errorw("value cannot be empty", "label_table", lbl)
		return fferr.NewInternalErrorf("value cannot be empty")
	}
	if lbl.SanitizedTableName == "" {
		logging.GlobalLogger.Errorw("sanitized table name cannot be empty", "label_table", lbl)
		return fferr.NewInternalErrorf("sanitized table name cannot be empty")
	}
	return nil
}

func createTableKey(tableName, entity, ts string) string {
	return fmt.Sprintf("%s_%s_%s", tableName, entity, ts)
}
