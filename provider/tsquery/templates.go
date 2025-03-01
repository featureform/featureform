package tsquery

const windowJoinLabelTemplate = `
WITH label_table AS (
  SELECT
    {{.labelTimestampColumn}} AS ts,
	{{.labelColumn}} AS label,

    {{.entities}}

  FROM {{.labelTable}}
),
`

const windowJoinFeatureTemplate = `
feature_{{.index}} AS (
  SELECT
    {{.featureTimestampColumn}} AS ts,
    label_table.{{.labelEntityColumn}},
    label_table.{{.labelColumn}},
    {{.valueColumns}}

    ROW_NUMBER() OVER (
      PARTITION BY l.{{.entityColumn}}, l.{{.timestampColumn}}
      ORDER BY feature_table{{.index}}.{{featureTimestampColumn}} DESC
    ) AS rn
    FROM label_table
    LEFT JOIN {{.featureTable}} feature_table{{.index}}
      ON feature_table.{{.featureEntityColumn}} = label_table.{{.labelEntityColumn}}
      AND feature_table{{.index}}.{{.featureTimestampColumn}} = label_table.ts
),

feature_{{.index}}_filtered AS (
  SELECT
    ts,
    {{.labelEntityColumn}},
    label,

    {{.valueColumns}},

    FROM feature_{{.index}}
    WHERE rn=1
)`
