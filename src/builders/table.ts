import type { Client } from "../client";
import { DatabaseTypes } from "./types";
import { NuvixException } from "../error";

export type NuvqlOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "like"
  | "ilike"
  | "in"
  | "nin"
  | "is"
  | "isnot"
  | "between"
  | "nbetween"
  | "contains"
  | "startswith"
  | "endswith";

export type NuvqlLogicalOperator = "and" | "or" | "not";

export interface NuvqlFilterCondition {
  column: string;
  operator: NuvqlOperator;
  value: any;
  isColumnReference?: boolean;
}

export interface NuvqlLogicalCondition {
  operator: NuvqlLogicalOperator;
  conditions: NuvqlCondition[];
}

export type NuvqlCondition = NuvqlFilterCondition | NuvqlLogicalCondition;

/**
 * Error types for query building
 */
export type QueryBuildError<T extends string> = {
  readonly __error: T;
  readonly __brand: "QueryBuildError";
};

// ============ TYPE UTILITIES ============

// Helper to check if a string is a valid column path (e.g., "table.column")
type IsValidColumnPath<T extends string> = T extends `${string}.${string}`
  ? true
  : false;

// Extract table name from column path
type ExtractTableName<T extends string> =
  T extends `${infer TableName}.${string}` ? TableName : never;

// Extract column name from column path
type ExtractColumnName<T extends string> =
  T extends `${string}.${infer ColumnName}` ? ColumnName : T;

// ============ UTILITY TYPES ============

type TableColumns<T extends DatabaseTypes.GenericTable> = keyof T["Row"];

type ColumnType<
  T extends DatabaseTypes.GenericTable,
  K extends TableColumns<T>,
> = T["Row"][K];

// Helper type for getting all available columns from current table + joined tables
type AllAvailableColumns<
  TTable extends DatabaseTypes.GenericTable,
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  > = {},
> =
  | TableColumns<TTable>
  | {
    [K in keyof TJoinedTables]: `${string & K}.${string &
    TableColumns<TJoinedTables[K]["table"]>}`;
  }[keyof TJoinedTables];

// Improved column value or reference type with better support for joined tables
type ColumnValueOrReference<
  TColumnType,
  TSchema extends DatabaseTypes.GenericSchema,
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  > = {},
> =
  | TColumnType
  | {
    [TableName in keyof TSchema["Tables"]]: `"${string & TableName}.${string &
    keyof TSchema["Tables"][TableName]["Row"]}"`;
  }[keyof TSchema["Tables"]]
  | {
    [JoinName in keyof TJoinedTables]: `"${string & JoinName}.${string &
    keyof TJoinedTables[JoinName]["table"]["Row"]}"`;
  }[keyof TJoinedTables];

// ============ COLUMN BUILDER ============

export type Cast =
  | "text"
  | "varchar"
  | "char"
  | "int"
  | "integer"
  | "bigint"
  | "smallint"
  | "float"
  | "real"
  | "double"
  | "numeric"
  | "decimal"
  | "boolean"
  | "bool"
  | "date"
  | "time"
  | "timestamp"
  | "timestamptz"
  | "json"
  | "jsonb"
  | "uuid"
  | string;

export type ValidateCast<TColumnType, TCast extends Cast> = TCast extends
  | "text"
  | "varchar"
  | "char"
  ? string
  : TCast extends
  | "int"
  | "integer"
  | "bigint"
  | "smallint"
  | "float"
  | "real"
  | "double"
  | "numeric"
  | "decimal"
  ? number
  : TCast extends "boolean" | "bool"
  ? boolean
  : TCast extends "date" | "time" | "timestamp" | "timestamptz"
  ? Date | string
  : TCast extends "json" | "jsonb"
  ? object | string
  : TCast extends "uuid"
  ? string
  : TColumnType;

export class ColumnBuilder<
  TColumn extends string = string,
  TAlias extends string | unknown = unknown,
  TCast extends Cast | unknown = unknown,
  TColumnType = unknown,
> {
  private readonly _column: TColumn;
  private readonly _alias?: TAlias;
  private readonly _castType?: TCast;
  private readonly _columnType?: TColumnType;
  private readonly _frozen: boolean = false;

  constructor(
    column: TColumn,
    options: {
      alias?: TAlias;
      castType?: TCast;
      columnType?: TColumnType;
      frozen?: boolean;
    } = {},
  ) {
    this._column = column;
    this._alias = options.alias;
    this._castType = options.castType;
    this._columnType = options.columnType;
    this._frozen = options.frozen ?? false;

    if (this._frozen) {
      Object.freeze(this);
    }
  }

  as<A extends string>(
    alias: A,
  ): ColumnBuilder<TColumn, A, TCast, TColumnType> {
    this._validateNotFrozen();
    return new ColumnBuilder(this._column, {
      alias,
      castType: this._castType,
      columnType: this._columnType,
      frozen: false,
    });
  }

  cast<C extends Cast>(
    castType: C,
  ): ColumnBuilder<TColumn, TAlias, C, ValidateCast<TColumnType, C>> {
    this._validateNotFrozen();
    this._validateCastCompatibility(castType);
    return new ColumnBuilder(this._column, {
      alias: this._alias,
      castType,
      columnType: {} as ValidateCast<TColumnType, C>,
      frozen: true, // Freeze after cast as type is now "finalized"
    });
  }

  toString(): string {
    let result = this._column as string;
    if (this._alias && typeof this._alias === "string") {
      result = `${result} as "${this._alias}"`;
    }
    if (this._castType && typeof this._castType === "string") {
      result = `${result}::${this._castType}`;
    }
    return result;
  }

  getResultType(): TCast extends Cast
    ? ValidateCast<TColumnType, TCast>
    : TColumnType {
    return {} as any; // Runtime value, type is for inference
  }

  getColumn(): TColumn {
    return this._column;
  }
  getAlias(): TAlias {
    return this._alias as TAlias;
  }
  getCast(): TCast {
    return this._castType as TCast;
  }
  getColumnType(): TColumnType {
    return this._columnType as TColumnType;
  }

  private _validateNotFrozen(): void {
    if (this._frozen) {
      throw new NuvixException(
        "Cannot modify a frozen ColumnBuilder. Use the returned instance from method calls.",
        400,
        "FROZEN_COLUMN_BUILDER",
      );
    }
  }

  private _validateCastCompatibility(castType: Cast): void {
    if (!castType || typeof castType !== "string") {
      throw new NuvixException(
        `Invalid cast type: ${castType}`,
        400,
        "INVALID_CAST_TYPE",
      );
    }
  }
}

export function column<
  TTable extends DatabaseTypes.GenericTable,
  TColumn extends TableColumns<TTable>,
>(
  name: TColumn,
): ColumnBuilder<
  TColumn & string,
  unknown,
  unknown,
  ColumnType<TTable, TColumn>
> {
  return new ColumnBuilder(name as string & TColumn, {
    columnType: {} as ColumnType<TTable, TColumn>,
  });
}

// Helper function for creating column references from joined tables
export function joinColumn<
  TJoinedTables extends Record<string, DatabaseTypes.GenericTable>,
  TJoinName extends keyof TJoinedTables & string,
  TColumn extends TableColumns<TJoinedTables[TJoinName]> & string,
>(
  joinName: TJoinName,
  columnName: TColumn,
): ColumnBuilder<
  `${TJoinName}.${TColumn}`,
  unknown,
  unknown,
  ColumnType<TJoinedTables[TJoinName], TColumn>
> {
  const fullName = `${joinName}.${columnName}` as `${TJoinName}.${TColumn}`;
  return new ColumnBuilder(fullName, {
    columnType: {} as ColumnType<TJoinedTables[TJoinName], TColumn>,
  });
}

// ============ SELECTION TYPES ============

// Improved JSON path types with proper recursion
type JsonPathSegment = string;
type JsonPathOperator = "->" | "->>";

// Support up to 5 levels of nesting for JSON paths
type Prev = [never, 0, 1, 2, 3, 4, 5];

type JsonPathString<T extends string, N extends number = 5> = N extends 0 
  ? never 
  : `${T}${JsonPathOperator}${JsonPathSegment}` 
  | `${T}->${JsonPathSegment}${JsonPathString<string, Prev[N]>}`;

type JsonPath<
  TTable extends DatabaseTypes.GenericTable,
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  > = {},
> = JsonPathString<TableColumns<TTable> & string> | JsonPathString<AllAvailableColumns<TTable, TJoinedTables> & string>;

// Helper to process JSON paths
function processJsonPath(path: string): { 
  baseColumn: string; 
  segments: { operator: JsonPathOperator; path: string }[];
  aliasName: string;
} {
  const parts = path.split(/(->>|->)/);
  const baseColumn = parts[0];
  const segments: { operator: JsonPathOperator; path: string }[] = [];
  
  for (let i = 1; i < parts.length; i += 2) {
    const operator = parts[i] as JsonPathOperator;
    const pathSegment = parts[i + 1];
    if (pathSegment) {
      segments.push({ operator, path: pathSegment.trim() });
    }
  }

  // Generate alias name by joining with underscores
  const aliasName = [
    baseColumn,
    ...segments.map(s => s.path)
  ].join('_');

  return { baseColumn, segments, aliasName };
}

// Better JSON path to field name conversion
export type JsonPathToFieldName<T extends string> =
  T extends `${infer Head}->>${infer Tail}`
  ? `${JsonPathToFieldName<Head>}_${Tail}`
  : T extends `${infer Head}->${infer Tail}`
  ? // We need to handle the tail recursively as well.
    JsonPathToFieldName<`${Head}_${JsonPathToFieldName<Tail>}`>
  : T;

function jsonPathToFieldName(path: string): string {
  // Replace all "->" and "->>" with "_"
  return path.replace(/->>?/g, '_');
}

// Helper to resolve column type from joined tables
type ResolveColumnType<
  TTable extends DatabaseTypes.GenericTable,
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  >,
  Col extends string,
> = Col extends TableColumns<TTable>
  ? ColumnType<TTable, Col>
  : Col extends `${infer JoinName}.${infer JoinCol}`
  ? JoinName extends keyof TJoinedTables
  ? JoinCol extends TableColumns<TJoinedTables[JoinName]["table"]>
  ? ColumnType<TJoinedTables[JoinName]["table"], JoinCol>
  : unknown
  : unknown
  : unknown;

type JsonValueType<TColType, TPath extends string> = TColType extends Record<
  string,
  any
>
  ? TPath extends keyof TColType
  ? TColType[TPath]
  : unknown // For deep paths beyond first level or unknown keys
  : unknown; // If not an object, JSON path extraction results in unknown

// Improved selection input type with better support for complex selections
type SelectionInput<
  T extends DatabaseTypes.GenericTable,
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  > = {},
> =
  | AllAvailableColumns<T, TJoinedTables> // Direct column or joined column
  | ColumnBuilder<TableColumns<T> & string, any, any, any> // ColumnBuilder instance
  | `${string}:${AllAvailableColumns<T, TJoinedTables> & string}` // alias:column
  | `${string}:${(
    | JsonPath<T, TJoinedTables>
    | `${AllAvailableColumns<T, TJoinedTables> & string}::${Cast}`
  ) &
  string}` // alias:json_path or alias:column::cast
  | JsonPath<T, TJoinedTables> // column->>json_path
  | `${AllAvailableColumns<T, TJoinedTables> & string}::${Cast}`; // column::cast

type ExtractSelectionType<
  TTable extends DatabaseTypes.GenericTable,
  TInput,
  TSchema extends DatabaseTypes.GenericSchema,
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  > = {},
> = TInput extends ColumnBuilder<infer C, infer A, infer CastType, infer ColType>
  ? // Case 1: ColumnBuilder instance
  A extends string
  ? { readonly [K in A]: CastType extends Cast ? ValidateCast<ColType, CastType> : ColType }
  : C extends AllAvailableColumns<TTable, TJoinedTables>
  ? { readonly [K in C & string]: ResolveColumnType<TTable, TJoinedTables, C & string> } // Should use ColType directly from builder
  : QueryBuildError<`ColumnBuilder points to an invalid column: '${C & string}'`>
  : TInput extends `${infer Alias}:${infer Rest}`
  ? // Case 3: Alias string 'alias:column', 'alias:column->path', 'alias:column::cast'
  Alias extends string
  ? Rest extends `${infer ColPath}::${infer CastType}`
  ? ColPath extends AllAvailableColumns<TTable, TJoinedTables>
  ? CastType extends Cast
  ? {
    readonly [K in Alias]: ValidateCast<
      ResolveColumnType<TTable, TJoinedTables, ColPath & string>,
      CastType
    >;
  }
  : QueryBuildError<`Invalid cast type '${CastType & string}' for aliased column '${Alias}:${ColPath & string}'`>
  : QueryBuildError<`Column '${ColPath & string}' not found for aliased column '${Alias}:${ColPath & string}'`>
  : Rest extends `${infer ColPath}->${infer Path}`
  ? ColPath extends AllAvailableColumns<TTable, TJoinedTables>
  ? {
    readonly [K in Alias]: JsonValueType<
      ResolveColumnType<TTable, TJoinedTables, ColPath & string>,
      Path & string
    >;
  }
  : QueryBuildError<`Column '${ColPath & string}' not found for aliased JSON path '${Alias}:${Rest & string}'`>
  : Rest extends AllAvailableColumns<TTable, TJoinedTables>
  ? { readonly [K in Alias]: ResolveColumnType<TTable, TJoinedTables, Rest & string> }
  : QueryBuildError<`Invalid column '${Rest & string}' in aliased selection '${Alias}:${Rest & string}'`>
  : QueryBuildError<`Invalid alias format: '${Alias & string}'`>
  : TInput extends `${infer ColPath}::${infer CastType}`
  ? // Case 4: Cast string 'column::cast'
  ColPath extends AllAvailableColumns<TTable, TJoinedTables>
  ? CastType extends Cast
  ? {
    readonly [K in ColPath & string]: ValidateCast<
      ResolveColumnType<TTable, TJoinedTables, ColPath & string>,
      CastType
    >;
  }
  : QueryBuildError<`Invalid cast type '${CastType & string}' for column '${ColPath & string}'`>
  : QueryBuildError<`Column '${ColPath & string}' not found for cast operation`>
  : TInput extends `${infer ColPath}->${infer Path}`
  ? // Case 5: JSON path string 'column->path' or 'column->>path'
  ColPath extends AllAvailableColumns<TTable, TJoinedTables>
  ? {
    readonly [K in JsonPathToFieldName<TInput & string>]: JsonValueType<
      ResolveColumnType<TTable, TJoinedTables, ColPath & string>,
      Path & string
    >;
  }
  : QueryBuildError<`Column '${ColPath & string}' not found for JSON path`>
  : TInput extends AllAvailableColumns<TTable, TJoinedTables>
  ? // Case 6: Direct column string 'column' or 'joinedTable.column'
  { readonly [K in TInput & string]: ResolveColumnType<TTable, TJoinedTables, TInput & string> }
  : QueryBuildError<`Invalid selection input: '${TInput & string}'`>;

// Improved selection merging with proper handling of overlapping keys
type MergeSelections<T extends readonly any[]> = DatabaseTypes.SimplifyDeep<
  T extends readonly [infer First, ...infer Rest]
  ? First extends Record<string, any>
  ? Rest extends readonly any[]
  ? DatabaseTypes.Prettify<First & MergeSelections<Rest>>
  : First
  : Rest extends readonly any[]
  ? MergeSelections<Rest>
  : {}
  : {}
>;

// ============ NEW JOIN & RESULT TYPES ============

// Utility to get the selection result from a builder instance type
type GetSelectionResult<TBuilder> =
  TBuilder extends TableQueryBuilder<any, any, any, infer TResult, any, any>
    ? TResult
    : never;

// Computes the shape of a single joined table in the final result
type ShapedJoinResult<
  TJoinName extends string,
  TJoinOptions extends JoinOptions,
  TJoinResult,
> = TJoinOptions extends { flatten: true }
  ? TJoinResult // Flattened properties are merged directly
  : {
      readonly [K in TJoinName]: TJoinOptions extends { shape: "one" }
        ? TJoinResult | null // Shaped as a single object
        : TJoinResult[]; // Default shape is an array of objects
    };

// Merges the main result with all joined results
type CombineWithJoins<TResult, TJoinedTables> = DatabaseTypes.Prettify<
  TResult &
    UnionToIntersection<
      {
        [K in keyof TJoinedTables]: K extends string
          ? TJoinedTables[K] extends {
              options: infer O extends JoinOptions;
              result: infer R;
            }
            ? ShapedJoinResult<K, O, R>
            : never
          : never;
      }[keyof TJoinedTables]
    >
>;

// ============ JOIN TYPES (Originals are mostly fine) ============

type JoinType = "left" | "inner" | "right" | "full";

interface FlattenJoin {
  flatten: true;
  type: JoinType;
}

interface ShapedJoin {
  type: JoinType;
  shape?: "one" | "many"; // Make shape optional with default 'many'
}

type JoinOptions<TTable extends string = string> = {
  table: TTable;
  as?: string;
} & (FlattenJoin | ShapedJoin);

// Simplified combined result type
type CombinedResultType<
  TResult,
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  >,
> = DatabaseTypes.Prettify<
  TResult & JoinedTablesResult<TJoinedTables> & FlattenedJoinProperties<TJoinedTables>
>;

// Simplified joined tables result type
type JoinedTablesResult<
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  >,
> = {
    [K in keyof TJoinedTables as TJoinedTables[K]["options"] extends {
      flatten: true;
    }
    ? never // Flattened joins do not appear as nested objects
    : TJoinedTables[K]["options"] extends { as: infer Alias }
    ? Alias extends string
    ? Alias
    : K // Use 'as' alias if present, else original join name
    : K]: TJoinedTables[K]["options"] extends { shape: "one" }
    ? TJoinedTables[K]["table"]["Row"] | null
    : TJoinedTables[K]["table"]["Row"][]; // Default to array of records
  };

// Simplified flattened properties from joins
type FlattenedJoinProperties<
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  >,
> = DatabaseTypes.Prettify<
  UnionToIntersection<
    {
      [K in keyof TJoinedTables]: TJoinedTables[K]["options"] extends {
        flatten: true;
      }
      ? TJoinedTables[K]["table"]["Row"]
      : {};
    }[keyof TJoinedTables]
  >
>;

// Helper for FlattenedJoinProperties
type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (
  k: infer I,
) => void
  ? I
  : never;

// Better joined tables tracking with options
type AddJoinedTableWithOptions<
  TJoinedTables extends Record<
    string,
    { table: DatabaseTypes.GenericTable; options: JoinOptions<string> }
  >,
  TJoinName extends string,
  TJoinTable extends DatabaseTypes.GenericTable,
  TJoinOptions extends JoinOptions<TJoinName>,
> = DatabaseTypes.Prettify<
  TJoinedTables & {
    [K in TJoinName]: {
      table: TJoinTable;
      options: TJoinOptions;
    };
  }
>;

// ============ QUERY BUILDER ============

export class TableQueryBuilder<
  TClient extends Client,
  TTable extends DatabaseTypes.GenericTable,
  TSchema extends DatabaseTypes.GenericSchema,
  TResult = TTable["Row"],
  TParentTable extends DatabaseTypes.GenericTable = TTable, // Not currently used, could be useful for nested queries
  TJoinedTables extends Record<string, {
    table: DatabaseTypes.GenericTable,
    options: JoinOptions<string>,
    result: any // Tracks the selection result of the join
  }> = {},
> {
  // --- IMMUTABLE STATE ---
  private readonly _client: TClient;
  private readonly _config: {
    readonly tableName: string;
    readonly schema: string;
    readonly isJoinBuilder?: boolean;
    readonly joinOptions?: JoinOptions<string>;
    readonly parentTableName?: string;
  };
  private readonly _selectedColumns: readonly string[];
  private readonly _conditions: readonly NuvqlCondition[];
  private readonly _joins: readonly { name: string; query: string }[];
  private readonly _joinedTables: TJoinedTables;

  constructor(
    client: TClient,
    config: {
      tableName: string;
      schema: string;
      isJoinBuilder?: boolean;
      joinOptions?: JoinOptions<string>;
      parentTableName?: string;
    },
    state?: {
      selectedColumns?: readonly string[];
      conditions?: readonly NuvqlCondition[];
      joins?: readonly { name: string; query: string }[];
      joinedTables?: TJoinedTables;
    }
  ) {
    this._client = client;
    this._config = config;
    this._selectedColumns = state?.selectedColumns ?? [];
    this._conditions = state?.conditions ?? [];
    this._joins = state?.joins ?? [];
    this._joinedTables = state?.joinedTables ?? ({} as TJoinedTables);
  }

  // --- INTERNAL CLONE METHOD FOR IMMUTABILITY ---
  private _clone<TNewResult = TResult>(
    newState: {
      selectedColumns?: readonly string[];
      conditions?: readonly NuvqlCondition[];
      joins?: readonly { name: string; query: string }[];
      joinedTables?: TJoinedTables;
    },
  ): TableQueryBuilder<TClient, TTable, TSchema, TNewResult, TParentTable, TJoinedTables> {
    return new TableQueryBuilder(this._client, this._config, {
      selectedColumns: newState.selectedColumns ?? this._selectedColumns,
      conditions: newState.conditions ?? this._conditions,
      joins: newState.joins ?? this._joins,
      joinedTables: newState.joinedTables ?? this._joinedTables,
    });
  }


  select(): TableQueryBuilder<
    TClient,
    TTable,
    TSchema,
    TTable["Row"],
    TParentTable,
    TJoinedTables
  >;
  select<const TSelections extends readonly SelectionInput<TTable, TJoinedTables>[]>(
    ...columns: TSelections
  ): TableQueryBuilder<
    TClient,
    TTable,
    TSchema,
    TSelections extends []
    ? TTable["Row"]
    : MergeSelections<{
      [I in keyof TSelections]: ExtractSelectionType<
        TTable,
        TSelections[I],
        TSchema,
        TJoinedTables
      >;
    }>,
    TParentTable,
    TJoinedTables
  >;
  select(...columns: any[]): any {
    if (columns.length === 0) {
      // Reset selection to all columns of the current table
      return this._clone({ selectedColumns: [] });
    }

    const newSelectedColumns: string[] = [];
    columns.forEach((column) => {
      if (column instanceof ColumnBuilder) {
        newSelectedColumns.push(column.toString());
      } else if (typeof column === "object" && column !== null) {
        Object.entries(column).forEach(([alias, colDef]) => {
          if (typeof colDef === "string") {
            newSelectedColumns.push(`${alias}:${colDef}`);
          }
        });
      } else if (typeof column === "string") {
        newSelectedColumns.push(column);
      }
    });

    return this._clone({ selectedColumns: newSelectedColumns });
  }

  join<
    const TJoinOptions extends JoinOptions<keyof TSchema["Tables"] & string>,
    const TJoinBuilder extends TableQueryBuilder<any, any, any, any, any, any>
  >(
    options: TJoinOptions,
    callback: (
      builder: TableQueryBuilder<
        TClient,
        TSchema["Tables"][TJoinOptions["table"]],
        TSchema,
        TSchema["Tables"][TJoinOptions["table"]]["Row"],
        TTable,
        TJoinedTables
      >,
    ) => TJoinBuilder,
  ): TableQueryBuilder<
    TClient,
    TTable,
    TSchema,
    CombineWithJoins<TResult, TJoinedTables & {
      [K in TJoinOptions["as"] extends string ? TJoinOptions["as"] : TJoinOptions["table"]]: {
        table: TSchema["Tables"][TJoinOptions["table"]];
        options: TJoinOptions;
        result: GetSelectionResult<TJoinBuilder>;
      }
    }>,
    TParentTable,
    TJoinedTables & {
      [K in TJoinOptions["as"] extends string ? TJoinOptions["as"] : TJoinOptions["table"]]: {
        table: TSchema["Tables"][TJoinOptions["table"]];
        options: TJoinOptions;
        result: GetSelectionResult<TJoinBuilder>;
      }
    }
  > {
    const table = options.table;
    const joinName = options.as || table;

    const joinBuilder = new TableQueryBuilder<TClient, TSchema["Tables"][TJoinOptions["table"]], TSchema, TSchema["Tables"][TJoinOptions["table"]]["Row"], TTable, TJoinedTables>(this._client, {
      tableName: table,
      schema: this._config.schema,
      isJoinBuilder: true,
      joinOptions: options,
      parentTableName: this._config.tableName,
    }, {
      // Pass down the parent's joined tables for nested filtering context
      joinedTables: this._joinedTables
    });

    const finalJoinBuilder = callback(joinBuilder);
    const joinQueryString = finalJoinBuilder.toString();

    const newJoinedTables = {
      ...this._joinedTables,
      [joinName]: {
        table: {} as TSchema["Tables"][TJoinOptions["table"]], // Type-level only
        options: options,
        result: {} as GetSelectionResult<TJoinBuilder> // Type-level only
      }
    };

    const newJoins = [...this._joins, { name: joinName, query: joinQueryString }];

    return new TableQueryBuilder(this._client, this._config, {
      selectedColumns: this._selectedColumns,
      conditions: this._conditions,
      joins: newJoins,
      joinedTables: newJoinedTables,
    });
  }

  eq<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "eq",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  neq<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "neq",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  gt<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "gt",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  gte<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "gte",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  lt<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "lt",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  lte<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "lte",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  like<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<string, TSchema, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "like",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  ilike<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<string, TSchema, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ilike",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  contains<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<string | any[], TSchema, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "contains",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  startswith<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<string, TSchema, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "startswith",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  endswith<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    value: ColumnValueOrReference<string, TSchema, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "endswith",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  in<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    values: ColumnValueOrReference<any, TSchema, TJoinedTables>[],
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "in",
      value: values,
    });
  }

  nin<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    values: ColumnValueOrReference<any, TSchema, TJoinedTables>[],
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "nin",
      value: values,
    });
  }

  between<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    min: ColumnValueOrReference<any, TSchema, TJoinedTables>,
    max: ColumnValueOrReference<any, TSchema, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "between",
      value: [min, max],
    });
  }

  nbetween<
    K extends AllAvailableColumns<TTable, TJoinedTables>,
  >(
    column: K,
    min: ColumnValueOrReference<any, TSchema, TJoinedTables>,
    max: ColumnValueOrReference<any, TSchema, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "nbetween",
      value: [min, max],
    });
  }

  is<K extends AllAvailableColumns<TTable, TJoinedTables>>(
    column: K,
    value: null | boolean | "null" | "not_null",
  ): this {
    return this.addCondition({ column: column as string, operator: "is", value });
  }

  isnot<K extends AllAvailableColumns<TTable, TJoinedTables>>(
    column: K,
    value: null | boolean | "null" | "not_null",
  ): this {
    return this.addCondition({ column: column as string, operator: "isnot", value });
  }

  isNull<K extends AllAvailableColumns<TTable, TJoinedTables>>(
    column: K,
  ): this {
    return this.is(column, "null");
  }

  isNotNull<K extends AllAvailableColumns<TTable, TJoinedTables>>(
    column: K,
  ): this {
    return this.is(column, "not_null");
  }

  // ============ LOGICAL OPERATORS ============

  and(
    callback: (
      filter: TableQueryBuilder<
        TClient,
        TTable,
        TSchema,
        TResult,
        TParentTable,
        TJoinedTables
      >,
    ) => any,
  ): this {
    const nestedFilter = new TableQueryBuilder<
      TClient,
      TTable,
      TSchema,
      TResult,
      TParentTable,
      TJoinedTables
    >(this._client, this._config, { joinedTables: this._joinedTables });
    
    callback(nestedFilter);

    if (nestedFilter.getConditions().length > 0) {
      return this.addLogicalCondition({
        operator: "and",
        conditions: nestedFilter.getConditions(),
      });
    }
    return this;
  }

  or(
    callback: (
      filter: TableQueryBuilder<
        TClient,
        TTable,
        TSchema,
        TResult,
        TParentTable,
        TJoinedTables
      >,
    ) => any,
  ): this {
    const nestedFilter = new TableQueryBuilder<
      TClient,
      TTable,
      TSchema,
      TResult,
      TParentTable,
      TJoinedTables
    >(this._client, this._config, { joinedTables: this._joinedTables });
    
    callback(nestedFilter);

    if (nestedFilter.getConditions().length > 0) {
      return this.addLogicalCondition({
        operator: "or",
        conditions: nestedFilter.getConditions(),
      });
    }
    return this;
  }

  not(
    callback: (
      filter: TableQueryBuilder<
        TClient,
        TTable,
        TSchema,
        TResult,
        TParentTable,
        TJoinedTables
      >,
    ) => any,
  ): this {
    const nestedFilter = new TableQueryBuilder<
      TClient,
      TTable,
      TSchema,
      TResult,
      TParentTable,
      TJoinedTables
    >(this._client, this._config, { joinedTables: this._joinedTables });
    
    callback(nestedFilter);

    if (nestedFilter.getConditions().length > 0) {
      return this.addLogicalCondition({
        operator: "not",
        conditions: nestedFilter.getConditions(),
      });
    }
    return this;
  }

  // ============ UTILITY METHODS ============

  protected isColumnReference(value: any): boolean {
    return (
      typeof value === "string" && value.startsWith('"') && value.endsWith('"')
    );
  }

  protected addCondition(condition: NuvqlFilterCondition): any {
    const newConditions = [...this._conditions, condition];
    return this._clone({ conditions: newConditions });
  }

  protected addLogicalCondition(condition: NuvqlLogicalCondition): any {
    const newConditions = [...this._conditions, condition];
    return this._clone({ conditions: newConditions });
  }

  getConditions(): NuvqlCondition[] {
    return [...this._conditions];
  }

  getJoinedTables(): TJoinedTables {
    return this._joinedTables;
  }

  // Helper method to access joined table columns with type safety
  getJoinedTableColumns<TJoinName extends keyof TJoinedTables>(
    joinName: TJoinName,
  ): TJoinedTables[TJoinName] extends { table: infer JTable extends DatabaseTypes.GenericTable }
    ? TableColumns<JTable>[]
    : never {
    // This is a runtime helper - actual column names would come from schema
    return [] as any;
  }

  // ============ QUERY EXECUTION ============

  async execute(): Promise<TResult[]> {
    // TODO: Implement actual query execution
    throw new NuvixException(
      "Query execution not implemented",
      500,
      "NOT_IMPLEMENTED",
    );
  }

  async single(): Promise<TResult> {
    const results = await this.execute();
    if (results.length === 0) {
      throw new NuvixException("No results found", 404, "NO_RESULTS");
    }
    if (results.length > 1) {
      throw new NuvixException(
        "Multiple results found",
        400,
        "MULTIPLE_RESULTS",
      );
    }
    return results[0];
  }

  async maybeSingle(): Promise<TResult | null> {
    const results = await this.execute();
    return results.length > 0 ? results[0] : null;
  }

  // ============ QUERY STRING BUILDING ============

  toString(): string {
    // Process selections to handle aliasing for JSON paths
    const processedSelect = this._selectedColumns.map(col => {
      if (col.includes("->") && !col.includes(":")) {
        return `${jsonPathToFieldName(col)}:${col}`;
      }
      return col;
    });

    const select = processedSelect.length === 0 ? "*" : processedSelect.join(",");
    const filter = this._conditions.map((c) => this.buildCondition(c)).join(",");
    const joins = this._joins.map(j => j.query).join(",");

    // If this is a join builder, the format is different.
    if (this._config.isJoinBuilder && this._config.joinOptions) {
      const opts = this._config.joinOptions;
      const type = `$.join(${opts.type ?? "inner"})`;
      const flatten = "flatten" in opts && opts.flatten ? "..." : "";
      const shape = !flatten && "shape" in opts ? `.${opts.shape ?? "many"}` : "";
      const alias = opts.as ? `${opts.as}:` : "";

      const conditions = [filter, type].filter(Boolean);

      let query = `${flatten}${alias}${this._config.tableName}${shape}`;

      if (select !== "*" || joins) {
        const innerSelect = [select, joins].filter(s => s !== "*").join(",");
        query += `(${innerSelect || "*"})`;
      }
      
      query += `{${conditions.join(",")}}`;

      return query;
    }

    // Main query format
    const finalSelect = [select, joins].filter(Boolean).join(",");
    let query = `select=${finalSelect}`;
    if (filter) {
      query += `&filter=${filter}`;
    }

    return query;
  }

  private buildCondition(condition: NuvqlCondition): string {
    if (
      "operator" in condition &&
      ["and", "or", "not"].includes(condition.operator)
    ) {
      const logical = condition as NuvqlLogicalCondition;
      const built = logical.conditions.map((c) => this.buildCondition(c));

      switch (logical.operator) {
        case "and":
          return `and(${built.join(",")})`;
        case "or":
          return `or(${built.join(",")})`;
        case "not":
          return `not(${built.join(",")})`;
        default:
          return built.join(",");
      }
    } else {
      const filter = condition as NuvqlFilterCondition;
      const { column, operator, value, isColumnReference } = filter;

      switch (operator) {
        case "eq":
          return `${column}.eq(${this.formatValue(value, isColumnReference)})`;
        case "neq":
          return `${column}.neq(${this.formatValue(value, isColumnReference)})`;
        case "gt":
          return `${column}.gt(${this.formatValue(value, isColumnReference)})`;
        case "gte":
          return `${column}.gte(${this.formatValue(value, isColumnReference)})`;
        case "lt":
          return `${column}.lt(${this.formatValue(value, isColumnReference)})`;
        case "lte":
          return `${column}.lte(${this.formatValue(value, isColumnReference)})`;
        case "like":
          return `${column}.like(${this.formatValue(value, isColumnReference)})`;
        case "ilike":
          return `${column}.ilike(${this.formatValue(value, isColumnReference)})`;
        case "contains":
          return `${column}.contains(${this.formatValue(value, isColumnReference)})`;
        case "startswith":
          return `${column}.startswith(${this.formatValue(value, isColumnReference)})`;
        case "endswith":
          return `${column}.endswith(${this.formatValue(value, isColumnReference)})`;
        case "in":
          const inValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value, isColumnReference);
          return `${column}.in[${inValues}]`;
        case "nin":
          const ninValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value, isColumnReference);
          return `${column}.nin[${ninValues}]`;
        case "between":
          const [min, max] = Array.isArray(value) ? value : [value, value];
          return `${column}.between[${this.formatValue(min)},${this.formatValue(max)}]`;
        case "nbetween":
          const [nMin, nMax] = Array.isArray(value) ? value : [value, value];
          return `${column}.nbetween[${this.formatValue(nMin)},${this.formatValue(nMax)}]`;
        case "is":
          return `${column}.is(${this.formatSpecialValue(value)})`;
        case "isnot":
          return `${column}.isnot(${this.formatSpecialValue(value)})`;
        default:
          return `${column}.eq(${this.formatValue(value, isColumnReference)})`;
      }
    }
  }

  private formatValue(value: any, isColumnReference?: boolean): string {
    // Handle column references
    if (
      isColumnReference &&
      typeof value === "string" &&
      value.startsWith('"') &&
      value.endsWith('"')
    ) {
      return value.slice(1, -1); // Remove quotes for column references
    }

    if (value === null || value === undefined) return "null";
    if (typeof value === "string") return `'${value.replace(/'/g, "\\'")}'`;
    if (typeof value === "boolean") return value ? "true" : "false";
    if (typeof value === "number") return String(value);
    if (value instanceof Date) return `'${value.toISOString()}'`;
    return `'${JSON.stringify(value).replace(/'/g, "\\'")}'`;
  }

  private formatSpecialValue(value: any): string {
    if (value === null || value === "null") return "null";
    if (value === "not_null") return "not_null";
    if (typeof value === "boolean") return value ? "true" : "false";
    return this.formatValue(value);
  }
}
