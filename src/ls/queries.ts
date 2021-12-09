/**
 * Copyright 2021 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { IBaseQueries, ContextValue } from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';

const describeTable: IBaseQueries['describeTable'] = queryFactory`
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = ''
  AND TABLE_SCHEMA  = '${p => p.schema}'
  AND TABLE_NAME    = '${p => p.label}'
`;

/**
 * Query that is used to fetch the columns of a single table/view. This is used in the object browser to
 * generate the column list of a table/view.
 */
const fetchColumns: IBaseQueries['fetchColumns'] = queryFactory`
SELECT
  C.COLUMN_NAME AS label,
  C.TABLE_NAME AS table,
  C.TABLE_SCHEMA AS schema,
  '${p => p.database}' AS database,
  C.SPANNER_TYPE AS dataType,
  C.SPANNER_TYPE AS detail,
  CASE
    WHEN STRPOS(SPANNER_TYPE, '(')=0 THEN NULL
    ELSE CAST(REPLACE(SUBSTR(C.SPANNER_TYPE, STRPOS(C.SPANNER_TYPE, '(')+1, STRPOS(C.SPANNER_TYPE, ')')-STRPOS(C.SPANNER_TYPE, '(')-1), 'MAX', CASE WHEN UPPER(C.SPANNER_TYPE) LIKE '%STRING%' THEN '2621440' ELSE '10485760' END) AS INT64)
  END AS size,
  CAST(C.COLUMN_DEFAULT AS STRING) AS defaultValue,
  CASE WHEN C.IS_NULLABLE = 'YES' THEN TRUE ELSE FALSE END AS isNullable,
  FALSE AS isPk,
  FALSE AS isFk,
  '${ContextValue.COLUMN}' as type
FROM INFORMATION_SCHEMA.COLUMNS AS C
WHERE TABLE_CATALOG = ''
AND   TABLE_SCHEMA  = '${p => p.schema}'
AND   TABLE_NAME    = '${p => p.label}'
ORDER BY ORDINAL_POSITION ASC
`;

const fetchRecords: IBaseQueries['fetchRecords'] = queryFactory`
SELECT *
FROM ${p => (p.table.label || p.table)}
LIMIT ${p => p.limit || 50}
OFFSET ${p => p.offset || 0};
`;

const countRecords: IBaseQueries['countRecords'] = queryFactory`
SELECT count(1) AS total
FROM ${p => (p.table.label || p.table)};
`;

/** Fetches the tables and views of a single schema. */
const fetchTablesAndViews = (type: ContextValue): IBaseQueries['fetchTables'] => queryFactory`
SELECT '${p => p.database}' AS database,
       TABLE_SCHEMA AS schema,
       TABLE_NAME  AS label,
       '${type}' AS type,
       ${type === ContextValue.VIEW ? 'TRUE' : 'FALSE'} AS isView
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = ''
AND   TABLE_SCHEMA  = '${p => p.schema}'
/* The default schema only contains tables, all other schemata only contain views. */
AND   CASE WHEN TABLE_SCHEMA='' THEN 'TABLE' ELSE 'VIEW' END = ${type === ContextValue.TABLE ? `'TABLE'` : `'VIEW'`}
ORDER BY TABLE_NAME
`;

const fetchTables: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.TABLE);
const fetchViews: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.VIEW);

/** Query that is used to search for available tables/views in a schema. */
const searchTables: IBaseQueries['searchTables'] = queryFactory`
SELECT CASE WHEN TABLE_SCHEMA='' THEN TABLE_NAME ELSE TABLE_SCHEMA || '.' || TABLE_NAME END AS label,
       CASE WHEN TABLE_SCHEMA='' THEN 'TABLE' ELSE 'VIEW' END AS type
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = ''
  ${p => p.search ? `AND (
    (TABLE_SCHEMA='' AND LOWER(TABLE_NAME) LIKE '%${p.search.toLowerCase()}%')
    OR
    (LOWER(TABLE_SCHEMA) || '.' || LOWER(TABLE_NAME)) LIKE '%${p.search.toLowerCase()}%'
  )`
  : ''}
ORDER BY TABLE_NAME
`;

/** Query that is used to search for available columns in a table/view. */
const searchColumns: IBaseQueries['searchColumns'] = queryFactory`
SELECT C.COLUMN_NAME AS label,
       C.TABLE_NAME AS table,
       C.SPANNER_TYPE AS dataType,
       CASE WHEN C.IS_NULLABLE = 'YES' THEN TRUE ELSE FALSE END AS isNullable,
       FALSE AS isPk,
       '${ContextValue.COLUMN}' as type
FROM INFORMATION_SCHEMA.COLUMNS C
WHERE 1 = 1
${p => p.tables.filter(t => !!t.label).length
  ? `AND LOWER(C.TABLE_NAME) IN (${p.tables.filter(t => !!t.label).map(t => `'${t.label}'`.toLowerCase()).join(', ')})`
  : ''
}
${p => p.search
  ? `AND (
    LOWER(C.TABLE_NAME || '.' || C.COLUMN_NAME) LIKE '%${p.search.toLowerCase()}%'
    OR LOWER(C.COLUMN_NAME) LIKE '%${p.search.toLowerCase()}%'
  )`
  : ''
}
ORDER BY C.COLUMN_NAME ASC, C.ORDINAL_POSITION ASC
LIMIT ${p => p.limit || 100}
`;

/**
 * Query for getting the schemata in a database. The default schema is nameless in Cloud Spanner. This
 * schema is returned with the label '(default)' by this query to make it easier to select and use in
 * the object browser.
 */
const fetchSchemas: IBaseQueries['fetchSchemas'] = queryFactory`
SELECT
  CASE WHEN SCHEMA_NAME = '' THEN '(default)' ELSE SCHEMA_NAME END AS label,
  SCHEMA_NAME AS schema,
  '${ContextValue.SCHEMA}' as type,
  'group-by-ref-type' as iconId,
  '${p => p.database}' as database
FROM INFORMATION_SCHEMA.SCHEMATA
`;

export default {
  describeTable,
  countRecords,
  fetchColumns,
  fetchRecords,
  fetchSchemas,
  fetchTables,
  fetchViews,
  searchTables,
  searchColumns
}
