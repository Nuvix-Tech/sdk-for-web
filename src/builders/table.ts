import type { Client } from "../client";
import { DatabaseTypes } from "./types";
import { NuvixException } from "../error";
import { Cast, ColumnBuilder, ValidateCast } from "./utils";

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

// ============ UTILITY TYPES ============

type TableColumns<T extends TableOrView> = keyof T["Row"];

type ColumnType<
  T extends TableOrView,
  K extends TableColumns<T>,
> = T["Row"][K];

// Helper type for getting all available columns from current table + joined tables
type AllAvailableColumns<
  TTable extends TableOrView,
  TJoinedTables extends Record<
    string,
    { table: TableOrView; options: JoinOptions<string> }
  > = {},
> =
  | TableColumns<TTable>
  | {
    [K in keyof TJoinedTables]: TJoinedTables[K]['options'] extends FlattenJoin
    ? TJoinedTables[K]['options']['flatten'] extends true
    ? `${string & K}.${string &
    TableColumns<TJoinedTables[K]["table"]>}` : never : never;
  }[keyof TJoinedTables];

type ColumnValueOrReference<
  TColumnType,
  TSchema extends DatabaseTypes.GenericSchema,
  TParentTable extends TableOrView,
  TJoinedTables extends Record<
    string,
    { table: TableOrView; options: JoinOptions<string> }
  > = {},
> =
  | TColumnType
  | {
    [TableName in keyof TSchema["Tables"]]: TSchema["Tables"][TableName] extends TParentTable ? `"${string & TableName}.${string &
    keyof TSchema["Tables"][TableName]["Row"]}"` : never;
  }[keyof TSchema["Tables"]]
  | {
    [JoinName in keyof TJoinedTables]: TJoinedTables[JoinName]['options'] extends FlattenJoin
    ? TJoinedTables[JoinName]['options']['flatten'] extends true ? `"${string & JoinName}.${string &
    keyof TJoinedTables[JoinName]["table"]["Row"]}"` : never : never;
  }[keyof TJoinedTables];

// ============ SELECTION TYPES ============

// Improved JSON path types with proper recursion
type JsonPathSegment = string;
type JsonPathOperator = "->" | "->>";

// Support up to 5 levels of nesting for JSON paths
type Prev = [never, 0, 1, 2, 3, 4, 5];

type JsonPathString<T extends string, N extends number = 5> = N extends 0
  ? never
  :
  | `${T}${JsonPathOperator}${JsonPathSegment}`
  | `${T}->${JsonPathSegment}${JsonPathString<string, Prev[N]>}`;

type JsonPath<
  TTable extends TableOrView,
  TJoinedTables extends Record<
    string,
    { table: TableOrView; options: JoinOptions<string> }
  > = {},
> =
  | JsonPathString<TableColumns<TTable> & string>
  | JsonPathString<AllAvailableColumns<TTable, TJoinedTables> & string>;

// Better JSON path to field name conversion
export type JsonPathToFieldName<T extends string> =
  T extends `${infer Head}->>${infer Tail}`
  ? `${JsonPathToFieldName<Head>}_${Tail}`
  : T extends `${infer Head}->${infer Tail}`
  ? // We need to handle the tail recursively as well.
  JsonPathToFieldName<`${Head}_${JsonPathToFieldName<Tail>}`>
  : T;

// Helper to resolve column type from joined tables
type ResolveColumnType<
  TTable extends TableOrView,
  TJoinedTables extends Record<
    string,
    { table: TableOrView; options: JoinOptions<string> }
  >,
  Col extends string,
> =
  Col extends TableColumns<TTable>
  ? ColumnType<TTable, Col>
  : Col extends `${infer JoinName}.${infer JoinCol}`
  ? JoinName extends keyof TJoinedTables
  ? JoinCol extends TableColumns<TJoinedTables[JoinName]["table"]>
  ? ColumnType<TJoinedTables[JoinName]["table"], JoinCol>
  : unknown
  : unknown
  : unknown;

type JsonValueType<TColType, TPath extends string> =
  TColType extends Record<string, any>
  ? TPath extends keyof TColType
  ? TColType[TPath]
  : unknown // For deep paths beyond first level or unknown keys
  : unknown; // If not an object, JSON path extraction results in unknown

// selection input type for complex selections
type SelectionInput<
  T extends TableOrView,
  TJoinedTables extends Record<
    string,
    { table: TableOrView; options: JoinOptions<string> }
  > = {},
> =
  | "*"
  | AllAvailableColumns<T, TJoinedTables> // Direct column or joined column
  | ColumnBuilder<
    (TableColumns<T> & string) | JsonPath<T, TJoinedTables>,
    any,
    any,
    any
  > // ColumnBuilder instance
  | `${string}:${AllAvailableColumns<T, TJoinedTables> & string}` // alias:column
  | `${string}:${(
    | JsonPath<T, TJoinedTables>
    | `${AllAvailableColumns<T, TJoinedTables> & string}::${Cast}`
  ) &
  string}` // alias:json_path or alias:column::cast
  | JsonPath<T, TJoinedTables> // column->>json_path
  | `${AllAvailableColumns<T, TJoinedTables> & string}::${Cast}`; // column::cast


// Gets the base column from a path string, e.g., "col->a->b" -> "col"
type GetBasePath<P extends string> = P extends `${infer Base}->${string}`
  ? Base
  : P;

// Gets the JSON path part of a string, e.g., "col->a->b" -> "a->b"
type GetJsonSubPath<P extends string> = P extends `${string}->${infer Sub}`
  ? Sub
  : never;

// Recursively resolves the type of a value from a JSON path
type RecursiveJsonValueType<
  TBase,
  TSubPath extends string,
> = TSubPath extends `${infer Key}->>${string}`
  ? string // Operator ->> always returns text
  : TSubPath extends `${infer Key}->${infer Rest}`
  ? TBase extends Record<string, any>
  ? Key extends keyof TBase
  ? RecursiveJsonValueType<TBase[Key], Rest>
  : unknown
  : unknown
  : TBase extends Record<string, any>
  ? TSubPath extends keyof TBase
  ? TBase[TSubPath]
  : unknown
  : unknown;

// Resolves the final type of a column or a full JSON path
type ResolvePathType<
  TTable extends TableOrView,
  TJoinedTables extends Record<string, any>,
  P extends string,
> =
  GetBasePath<P> extends infer BasePath
  ? BasePath extends AllAvailableColumns<TTable, TJoinedTables>
  ? ResolveColumnType<
    TTable,
    TJoinedTables,
    BasePath & string
  > extends infer ColType
  ? P extends `${string}->${string}`
  ? RecursiveJsonValueType<ColType, GetJsonSubPath<P>>
  : ColType // This was a simple column path
  : unknown
  : unknown
  : never;

// Parses a selection string into its constituent parts: Alias, Path, and Cast
type ParseSelection<S extends string> = S extends `${infer Rest}::${infer C}`
  ? Rest extends `${infer Alias}:${infer Path}`
  ? C extends Cast
  ? { Alias: Alias; Path: Path; Cast: C }
  : { Alias: Alias; Path: Path; Cast: unknown }
  : C extends Cast
  ? { Alias: JsonPathToFieldName<Rest>; Path: Rest; Cast: C }
  : { Alias: JsonPathToFieldName<Rest>; Path: Rest; Cast: unknown }
  : S extends `${infer Alias}:${infer Path}`
  ? { Alias: Alias; Path: Path; Cast: unknown }
  : { Alias: JsonPathToFieldName<S>; Path: S; Cast: unknown };


type ExtractSelectionType<
  TTable extends TableOrView,
  TInput,
  TSchema extends DatabaseTypes.GenericSchema, // not used
  TJoinedTables extends Record<
    string,
    { table: TableOrView; options: JoinOptions<string> }
  > = {},
> =
  // Case 1: ColumnBuilder
  TInput extends "*"
  ? Readonly<TTable["Row"]>
  : TInput extends ColumnBuilder<infer C, infer A, infer Cast, any>
  ? C extends string
  ? {
    readonly [K in A extends string
    ? A
    : JsonPathToFieldName<C>]: Cast extends Cast & string
    ? ValidateCast<ResolvePathType<TTable, TJoinedTables, C>, Cast>
    : ResolvePathType<TTable, TJoinedTables, C>;
  }
  : QueryBuildError<"Invalid ColumnBuilder">
  : // Case 2: String literal
  TInput extends string
  ? ParseSelection<TInput> extends {
    Alias: infer A;
    Path: infer P;
    Cast: infer C;
  }
  ? P extends string
  ? ResolvePathType<TTable, TJoinedTables, P> extends infer PathType
  ? PathType extends QueryBuildError<any>
  ? PathType
  : {
    readonly [K in A extends string ? A : never]: C extends Cast
    ? ValidateCast<PathType, C & string>
    : PathType;
  }
  : QueryBuildError<`Could not resolve path type for '${P}'`>
  : QueryBuildError<`Invalid path in selection: ${TInput}`>
  : QueryBuildError<`Could not parse selection string: ${TInput}`>
  : QueryBuildError<"Invalid selection input">;

// selection merging 
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

// ============ JOIN & RESULT TYPES ============

// Utility to get the selection result from a builder instance type
type GetSelectionResult<TBuilder> =
  TBuilder extends TableQueryBuilder<any, any, any, infer TResult, any, any>
  ? TResult
  : never;

// Computes the kind of a single joined table in the final result
type ShapedJoinResult<
  TJoinName extends string,
  TJoinOptions extends JoinOptions,
  TJoinResult,
> = TJoinOptions extends { flatten: true }
  ? TJoinResult // Flattened properties are merged directly
  : {
    readonly [K in TJoinName]: TJoinOptions extends { kind: "one" }
    ? TJoinResult | null // Shaped as a single object
    : TJoinResult[]; // Default kind is an array of objects
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

type JoinType = "left" | "inner" | "right" | "full";

interface FlattenJoin {
  flatten: true;
  type?: JoinType;
}

interface ShapedJoin {
  type?: Omit<JoinType, "right" | "full">;
  kind?: "one" | "many"; // Make kind optional with default 'many'
}

type JoinOptions<TTable extends string = string> = {
  table: TTable;
  as?: string;
} & (FlattenJoin | ShapedJoin);

// Helper for FlattenedJoinProperties
type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (
  k: infer I,
) => void
  ? I
  : never;

// ============ QUERY BUILDER ============

type TableOrView = DatabaseTypes.GenericTable | DatabaseTypes.GenericView | DatabaseTypes.GenericUpdatableView;

export class TableQueryBuilder<
  TClient extends Client,
  TTable extends TableOrView,
  TSchema extends DatabaseTypes.GenericSchema,
  TResult = TTable["Row"],
  TParentTable extends TableOrView = TTable, // Not currently used, could be useful for nested queries
  TJoinedTables extends Record<
    string,
    {
      table: TableOrView;
      options: JoinOptions<string>;
      result: any; // Tracks the selection result of the join
    }
  > = {},
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
    },
  ) {
    this._client = client;
    this._config = config;
    this._selectedColumns = state?.selectedColumns ?? [];
    this._conditions = state?.conditions ?? [];
    this._joins = state?.joins ?? [];
    this._joinedTables = state?.joinedTables ?? ({} as TJoinedTables);
  }

  // --- INTERNAL CLONE METHOD FOR IMMUTABILITY ---
  private _clone<TNewResult = TResult>(newState: {
    selectedColumns?: readonly string[];
    conditions?: readonly NuvqlCondition[];
    joins?: readonly { name: string; query: string }[];
    joinedTables?: TJoinedTables;
  }): TableQueryBuilder<
    TClient,
    TTable,
    TSchema,
    TNewResult,
    TParentTable,
    TJoinedTables
  > {
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
    Readonly<TTable["Row"]>,
    TParentTable,
    TJoinedTables
  >;
  select<
    const TSelections extends readonly SelectionInput<TTable, TJoinedTables>[],
  >(
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

  /**
   * Joins another table or view to the current query.
   *
   * @param options - Join options including table name, alias, and join type.
   * @param callback - Callback to build the join query.
   */
  join<
    const TJoinOptions extends JoinOptions<keyof TSchema["Tables"] & string>,
    const TJoinBuilder extends TableQueryBuilder<any, any, any, any, any, any>,
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
    CombineWithJoins<
      TResult,
      TJoinedTables & {
        [K in TJoinOptions["as"] extends string
        ? TJoinOptions["as"]
        : TJoinOptions["table"]]: {
          table: TSchema["Tables"][TJoinOptions["table"]];
          options: TJoinOptions;
          result: GetSelectionResult<TJoinBuilder>;
        };
      }
    >,
    TParentTable,
    TJoinedTables & {
      [K in TJoinOptions["as"] extends string
      ? TJoinOptions["as"]
      : TJoinOptions["table"]]: {
        table: TSchema["Tables"][TJoinOptions["table"]];
        options: TJoinOptions;
        result: GetSelectionResult<TJoinBuilder>;
      };
    }
  > {
    const table = options.table;
    const joinName = options.as || table;

    const joinBuilder = new TableQueryBuilder<
      TClient,
      TSchema["Tables"][TJoinOptions["table"]],
      TSchema,
      TSchema["Tables"][TJoinOptions["table"]]["Row"],
      TTable,
      TJoinedTables
    >(
      this._client,
      {
        tableName: table,
        schema: this._config.schema,
        isJoinBuilder: true,
        joinOptions: options,
        parentTableName: this._config.tableName,
      },
      {
        // Pass down the parent's joined tables for nested filtering context
        joinedTables: this._joinedTables,
      },
    );

    const finalJoinBuilder = callback(joinBuilder);
    const joinQueryString = finalJoinBuilder.toString();

    const newJoinedTables = {
      ...this._joinedTables,
      [joinName]: {
        table: {} as TSchema["Tables"][TJoinOptions["table"]], // Type-level only
        options: options,
        result: {} as GetSelectionResult<TJoinBuilder>, // Type-level only
      },
    };

    const newJoins = [
      ...this._joins,
      { name: joinName, query: joinQueryString },
    ];

    return new TableQueryBuilder(this._client, this._config, {
      selectedColumns: this._selectedColumns,
      conditions: this._conditions,
      joins: newJoins,
      joinedTables: newJoinedTables,
    } as any); // Use 'as any' to bypass the complex type checking here, since we know it's correct
  }

  eq<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
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

  neq<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
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

  gt<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
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

  gte<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
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

  lt<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
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

  lte<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
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

  like<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<string, TSchema,
      TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "like",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  ilike<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<string, TSchema,
      TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ilike",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  contains<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<string | any[], TSchema,
      TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "contains",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  startswith<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<string, TSchema,
      TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "startswith",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  endswith<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<string, TSchema,
      TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "endswith",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  in<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    values: ColumnValueOrReference<any, TSchema,
      TParentTable, TJoinedTables>[],
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "in",
      value: values,
    });
  }

  nin<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    values: ColumnValueOrReference<any, TSchema,
      TParentTable, TJoinedTables>[],
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "nin",
      value: values,
    });
  }

  between<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    min: ColumnValueOrReference<any, TSchema,
      TParentTable, TJoinedTables>,
    max: ColumnValueOrReference<any, TSchema,
      TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "between",
      value: [min, max],
    });
  }

  nbetween<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    min: ColumnValueOrReference<any, TSchema,
      TParentTable, TJoinedTables>,
    max: ColumnValueOrReference<any, TSchema,
      TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "nbetween",
      value: [min, max],
    });
  }

  is<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: null | boolean | "null" | "not_null",
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "is",
      value,
    });
  }

  isnot<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
    value: null | boolean | "null" | "not_null",
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "isnot",
      value,
    });
  }

  isNull<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
    column: K,
  ): this {
    return this.is(column, "null");
  }

  isNotNull<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable>>(
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
  ): TJoinedTables[TJoinName] extends {
    table: infer JTable extends TableOrView;
  }
    ? TableColumns<JTable>[]
    : never {
    // This is a runtime helper - actual column names would come from schema
    return [] as any;
  }

  // ============ QUERY EXECUTION ============

  async execute(): Promise<TResult[]> {
    const query = new URLSearchParams(this.toString())
    const url = new URL(`${this._client.config.endpoint}/schemas/${this._config.schema}/${this._config.tableName}`);
    url.search = query.toString();
    const response = await this._client.call('GET', url)
    return response;
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
    const select =
      this._selectedColumns.length === 0
        ? "*"
        : this._selectedColumns.join(",");
    const filter = this._conditions
      .map((c) => this.buildCondition(c))
      .join(",");
    const joins = this._joins.map((j) => j.query).join(",");

    // If this is a join builder, the format is different.
    if (this._config.isJoinBuilder && this._config.joinOptions) {
      const opts = this._config.joinOptions;
      const type = `$.join(${opts.type ?? "inner"})`;
      const flatten = "flatten" in opts && opts.flatten ? "..." : "";
      const kind =
        !flatten && "kind" in opts ? `.${opts.kind ?? "many"}` : "";
      const alias = opts.as ? `${opts.as}:` : "";

      const conditions = [filter, type].filter(Boolean);

      let query = `${flatten}${alias}${this._config.tableName}${kind}`;

      query += `{${conditions.join(",")}}`;

      if (select !== "*" || joins) {
        const innerSelect = [select, joins].filter((s) => s !== "*").join(",");
        query += `(${innerSelect || "*"})`;
      }
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
      return value; // Return as is if it's a column reference
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
