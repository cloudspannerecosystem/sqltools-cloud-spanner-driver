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

import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import {Database, Spanner, SpannerOptions} from '@google-cloud/spanner';
import { RunUpdateResponse } from '@google-cloud/spanner/build/src/transaction';
import { SpannerQueryParser, StatementType } from './parser';

type DriverLib = Database;
type DriverOptions = SpannerOptions;
const MAX_QUERY_RESULTS = 100000;

export default class CloudSpannerDriver extends AbstractDriver<DriverLib, DriverOptions> implements IConnectionDriver {
  private _databaseId: string;
  queries = queries;

  public async open() {
    if (this.connection) {
      return this.connection;
    }
    const options = {} as SpannerOptions;
    options.projectId = this.credentials.project;
    options.keyFile = this.credentials.credentialsKeyFile;
    const spanner = new Spanner(options);
    const instance = spanner.instance(this.credentials.instance);
    const database = instance.database(this.credentials.database);
    this._databaseId = this.credentials.database;

    this.connection = Promise.resolve(database);
    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    const database = await this.connection;
    await database.close();
    this.connection = null;
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (queries, opt = {}) => {
    const db = await this.open();
    const resultsAgg: NSDatabase.IResult[] = [];
    const statementsArray = SpannerQueryParser.parse(queries.toString());
    for (const sql of statementsArray) {
      const statementType = SpannerQueryParser.getStatementType(sql);
      switch (statementType) {
        case StatementType.QUERY:
          resultsAgg.push(await this.executeQuery(db, sql, opt));
          break;
        case StatementType.DML:
          resultsAgg.push(await this.executeDml(db, sql, opt));
          break;
        case StatementType.DDL:
        case StatementType.UNSPECIFIED:
          throw new Error(`Unsupported statement: ${sql}`);
      }
    }
    return resultsAgg;
  }

  private async executeQuery(db: Database, sql: string, opt): Promise<NSDatabase.IResult> {
    const countQuery = `SELECT COUNT(*) FROM (${sql})`;
    const [count] = await db.run(countQuery);
    const recordCount = count[0][0].value.value;
    if (recordCount > MAX_QUERY_RESULTS) {
      return {
        cols: ['Error'],
        connId: this.getId(),
        messages: [{ date: new Date(), message: `Query result is too large with ${recordCount} results. Limit the query results to max ${MAX_QUERY_RESULTS} and rerun the query.`}],
        results: [{Error: `Query result is too large with ${recordCount} results. Limit the query results to max ${MAX_QUERY_RESULTS} and rerun the query.`}],
        query: sql,
        requestId: opt.requestId,
        resultId: generateId(),
      };
    }
    const [rows, , metadata] = await db.run({sql, json: true, jsonOptions: {wrapNumbers: true, includeNameless: true}});
    const cols = metadata.rowType.fields.map((field, index) => field.name ? field.name : `_${index}`);
    return {
      cols,
      connId: this.getId(),
      messages: [{ date: new Date(), message: `Query ok with ${rows.length} results`}],
      results: this.mapRows(rows, cols),
      query: sql,
      requestId: opt.requestId,
      resultId: generateId(),
    };
  }

  private async executeDml(db: Database, sql: string, opt): Promise<NSDatabase.IResult> {
    const [rowCount] = await db.runTransactionAsync((transaction): Promise<RunUpdateResponse> => {
      return transaction.runUpdate(sql);
    });
    return {
      cols: ['rowCount'],
      connId: this.getId(),
      messages: [{ date: new Date(), message: `Update ok with ${rowCount} updated rows`}],
      results: [{rowCount}],
      query: sql,
      requestId: opt.requestId,
      resultId: generateId(),
    };
  }

  private mapRows(rows: any[], columns: string[]): any[] {
    return rows.map((r) => {
      columns.forEach((col) => {
        if (r[col] && r[col].value) {
          r[col] = r[col].value;
        }
      });
      return r;
    });
  }

  public async testConnection() {
    await this.open();
    await this.query('SELECT 1', {});
  }

  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return this.queryResults(queries.fetchSchemas({database: this._databaseId} as NSDatabase.IDatabase));
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(queries.fetchColumns(item as NSDatabase.ITable))
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.childType) {
      case ContextValue.TABLE:
        return this.queryResults(queries.fetchTables(parent as NSDatabase.ISchema));
      case ContextValue.VIEW:
        return this.queryResults(queries.fetchViews(parent as NSDatabase.ISchema));
    }
    return [];
  }

  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(queries.searchTables({search}));
      case ContextValue.COLUMN:
        return this.queryResults(queries.searchColumns({search, ..._extraParams}));
    }
    return [];
  }

  private sqlKeywords = 'SELECT,WITH,INSERT,UPDATE,DELETE,CREATE,ALTER,DROP';
  private numericFunctions = "ABS,SIGN,IS_INF,IS_NAN,IEEE_DIVIDE,SQRT,POW,POWER,EXP,LN,LOG,LOG10,GREATEST,LEAST,DIV,MOD,ROUND,TRUNC,CEIL,CEILING,FLOOR,COS,COSH,ACOS,ACOSH,SIN,SINH,ASIN,ASINH,TAN,TANH,ATAN,ATANH,ATAN2,FARM_FINGERPRINT,SHA1,SHA256,SHA512";
  private stringFunctions =  "BYTE_LENGTH,CHAR_LENGTH,CHARACTER_LENGTH,CODE_POINTS_TO_BYTES,CODE_POINTS_TO_STRING,CONCAT,ENDS_WITH,FORMAT,FROM_BASE64,FROM_HEX,LENGTH,LPAD,LOWER,LTRIM,REGEXP_CONTAINS,REGEXP_EXTRACT,REGEXP_EXTRACT_ALL,REGEXP_REPLACE,REPLACE,REPEAT,REVERSE,RPAD,RTRIM,SAFE_CONVERT_BYTES_TO_STRING,SPLIT,STARTS_WITH,STRPOS,SUBSTR,TO_BASE64,TO_CODE_POINTS,TO_HEX,TRIM,UPPER,JSON_QUERY,JSON_VALUE";
  private dateFunctions = "CURRENT_DATE,EXTRACT,DATE,DATE_ADD,DATE_SUB,DATE_DIFF,DATE_TRUNC,DATE_FROM_UNIX_DATE,FORMAT_DATE,PARSE_DATE,UNIX_DATE,CURRENT_TIMESTAMP,STRING,TIMESTAMP,TIMESTAMP_ADD,TIMESTAMP_SUB,TIMESTAMP_DIFF,TIMESTAMP_TRUNC,FORMAT_TIMESTAMP,PARSE_TIMESTAMP,TIMESTAMP_SECONDS,TIMESTAMP_MILLIS,TIMESTAMP_MICROS,UNIX_SECONDS,UNIX_MILLIS,UNIX_MICROS";
  private completionsCache: { [w: string]: NSDatabase.IStaticCompletion } = null;

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    if (this.completionsCache) return this.completionsCache;

    this.completionsCache = {};
    const allFunctions = this.sqlKeywords + ',' + this.numericFunctions + ',' + this.stringFunctions + ',' + this.dateFunctions;
    allFunctions.split(',').forEach(f => {
      this.completionsCache[f] = {
        label: f,
        detail: f,
        filterText: f,
        sortText: (this.sqlKeywords.includes(f) ? '2:' : '') + f,
        documentation: {
          kind: 'markdown',
          value: f,
        },
      }
    });
    return this.completionsCache;
  }
}
