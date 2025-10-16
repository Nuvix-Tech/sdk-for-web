import type { BaseClient } from "../base-client";
import { DatabaseTypes } from "./types";
import { NuvixException } from "../error";
import { Cast, Column, ColumnBuilder, ValidateCast } from "./utils";
import { ResponseType } from "../type";

export type NuvqlOperator =
  // Comparison operators
  | "eq" // =
  | "neq" // <> or !=
  | "gt" // >
  | "gte" // >=
  | "lt" // <
  | "lte" // <=
  // Comparison with quantifiers
  | "eqAny"
  | "eqAll"
  | "neqAny"
  | "neqAll"
  | "gtAny"
  | "gtAll"
  | "gteAny"
  | "gteAll"
  | "ltAny"
  | "ltAll"
  | "lteAny"
  | "lteAll"
  // String operators
  | "like" // LIKE
  | "ilike" // ILIKE
  | "match" // ~ (regex match)
  | "imatch" // ~* (case-insensitive regex match)
  // String with quantifiers
  | "likeAny"
  | "likeAll"
  | "ilikeAny"
  | "ilikeAll"
  | "matchAny"
  | "matchAll"
  | "imatchAny"
  | "imatchAll"
  // Array/List operators
  | "in" // IN
  | "notin" // NOT IN
  | "ov" // && (overlaps)
  | "cs" // @> (contains)
  | "cd" // <@ (contained by)
  // Range operators
  | "between" // BETWEEN
  | "nbetween" // NOT BETWEEN
  | "sl" // << (strictly left of)
  | "sr" // >> (strictly right of)
  | "nxr" // &< (does not extend right of)
  | "nxl" // &> (does not extend left of)
  | "adj" // -|- (adjacent to)
  // Null operators
  | "is" // IS
  | "isnot" // IS NOT
  | "null" // IS NULL
  | "notnull" // IS NOT NULL
  | "isdistinct" // IS DISTINCT FROM
  // Full-Text Search
  | "fts" // @@ (to_tsquery)
  | "plfts" // @@ (plainto_tsquery)
  | "phfts" // @@ (phraseto_tsquery)
  | "wfts" // @@ (websearch_to_tsquery)
  // Misc operators
  | "all" // ALL
  | "any"; // ANY

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

type ColumnType<T extends TableOrView, K extends TableColumns<T>> = T["Row"][K];

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
      [K in keyof TJoinedTables]: `${string & K}.${string & TableColumns<TJoinedTables[K]["table"]>}`;
    }[keyof TJoinedTables];

type AllAvailableGroupColumns<
  TTable extends TableOrView,
  TJoinedTables extends Record<
    string,
    { table: TableOrView; options: JoinOptions<string> }
  > = {},
> =
  | TableColumns<TTable>
  | {
      [K in keyof TJoinedTables]: TJoinedTables[K]["options"] extends {
        flatten: true;
      }
        ? `${string & K}.${string & TableColumns<TJoinedTables[K]["table"]>}`
        : never;
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
      [TableName in keyof TSchema["Tables"]]: TSchema["Tables"][TableName] extends TParentTable
        ? `"${string & TableName}.${string &
            keyof TSchema["Tables"][TableName]["Row"]}"`
        : never;
    }[keyof TSchema["Tables"]]
  | {
      [JoinName in keyof TJoinedTables]: TJoinedTables[JoinName]["options"] extends {
        flatten: true;
      }
        ? `"${string & JoinName}.${string &
            keyof TJoinedTables[JoinName]["table"]["Row"]}"`
        : never;
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

type OrderObject<TTable extends TableOrView> = {
  [k in TableColumns<TTable> | JsonPath<TTable>]?:
    | "asc"
    | "desc"
    | `${"asc" | "desc"}.${"nullsfirst" | "nullslast"}`
    | "nullsfirst"
    | "nullslast";
};

// ============ QUERY BUILDER ============

type TableOrView =
  | DatabaseTypes.GenericTable
  | DatabaseTypes.GenericView
  | DatabaseTypes.GenericUpdatableView;

export class TableQueryBuilder<
  TClient extends BaseClient,
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
  TResultFinal = TResult[],
> implements PromiseLike<ResponseType<TClient, TResultFinal>>
{
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
  private _extra: {
    limit?: number;
    orders?: string[];
    offset?: number;
    groupBy?: string[];
  } = {};
  private _single?: boolean = false;
  private _maybeSingle?: boolean = false;

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
      if (column instanceof Column) {
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
    });
  }

  eq<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
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

  neq<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
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

  gt<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
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

  gte<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
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

  lt<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
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

  lte<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
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

  like<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: string,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "like",
      value,
    });
  }

  ilike<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: string,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ilike",
      value,
    });
  }

  in<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    values: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>[],
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "in",
      value: values,
    });
  }

  between<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    min: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
    max: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "between",
      value: [min, max],
    });
  }

  nbetween<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    min: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
    max: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "nbetween",
      value: [min, max],
    });
  }

  is<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: null | boolean | "null" | "not_null",
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "is",
      value,
    });
  }

  isnot<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: null | boolean | "null" | "not_null",
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "isnot",
      value,
    });
  }

  isNull<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
  ): this {
    return this.is(column, "null");
  }

  isNotNull<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
  ): this {
    return this.is(column, "not_null");
  }

  // ============ COMPARISON OPERATORS WITH QUANTIFIERS ============

  /**
   * Filter resources where column equals ANY of the provided values.
   * Generates: column.eq(any,value1,value2,...)
   */
  eqAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "eqAny",
      value: values,
    });
  }

  /**
   * Filter resources where column equals ALL of the provided values.
   * Generates: column.eq(all,value1,value2,...)
   */
  eqAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "eqAll",
      value: values,
    });
  }

  /**
   * Filter resources where column not equals ANY of the provided values.
   * Generates: column.neq(any,value1,value2,...)
   */
  neqAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "neqAny",
      value: values,
    });
  }

  /**
   * Filter resources where column not equals ALL of the provided values.
   * Generates: column.neq(all,value1,value2,...)
   */
  neqAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "neqAll",
      value: values,
    });
  }

  /**
   * Filter resources where column is greater than ANY of the provided values.
   * Generates: column.gt(any,value1,value2,...)
   */
  gtAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "gtAny",
      value: values,
    });
  }

  /**
   * Filter resources where column is greater than ALL of the provided values.
   * Generates: column.gt(all,value1,value2,...)
   */
  gtAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "gtAll",
      value: values,
    });
  }

  /**
   * Filter resources where column is greater than or equal to ANY of the provided values.
   * Generates: column.gte(any,value1,value2,...)
   */
  gteAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "gteAny",
      value: values,
    });
  }

  /**
   * Filter resources where column is greater than or equal to ALL of the provided values.
   * Generates: column.gte(all,value1,value2,...)
   */
  gteAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "gteAll",
      value: values,
    });
  }

  /**
   * Filter resources where column is less than ANY of the provided values.
   * Generates: column.lt(any,value1,value2,...)
   */
  ltAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ltAny",
      value: values,
    });
  }

  /**
   * Filter resources where column is less than ALL of the provided values.
   * Generates: column.lt(all,value1,value2,...)
   */
  ltAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ltAll",
      value: values,
    });
  }

  /**
   * Filter resources where column is less than or equal to ANY of the provided values.
   * Generates: column.lte(any,value1,value2,...)
   */
  lteAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "lteAny",
      value: values,
    });
  }

  /**
   * Filter resources where column is less than or equal to ALL of the provided values.
   * Generates: column.lte(all,value1,value2,...)
   */
  lteAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...values: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "lteAll",
      value: values,
    });
  }

  // ============ STRING OPERATORS ============

  /**
   * Filter resources where attribute matches the regex pattern.
   * Uses PostgreSQL ~ operator for case-sensitive regex matching.
   *
   * @param column - The column to filter on
   * @param pattern - The regex pattern to match
   */
  match<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    pattern: string,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "match",
      value: pattern,
    });
  }

  /**
   * Filter resources where attribute matches the regex pattern (case-insensitive).
   * Uses PostgreSQL ~* operator for case-insensitive regex matching.
   *
   * @param column - The column to filter on
   * @param pattern - The regex pattern to match
   */
  imatch<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    pattern: string,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "imatch",
      value: pattern,
    });
  }

  // ============ STRING OPERATORS WITH QUANTIFIERS ============

  /**
   * Filter resources where column LIKE ANY of the provided patterns.
   * Generates: column.like(any,pattern1,pattern2,...)
   */
  likeAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "likeAny",
      value: patterns,
    });
  }

  /**
   * Filter resources where column LIKE ALL of the provided patterns.
   * Generates: column.like(all,pattern1,pattern2,...)
   */
  likeAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "likeAll",
      value: patterns,
    });
  }

  /**
   * Filter resources where column ILIKE ANY of the provided patterns.
   * Generates: column.ilike(any,pattern1,pattern2,...)
   */
  ilikeAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ilikeAny",
      value: patterns,
    });
  }

  /**
   * Filter resources where column ILIKE ALL of the provided patterns.
   * Generates: column.ilike(all,pattern1,pattern2,...)
   */
  ilikeAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ilikeAll",
      value: patterns,
    });
  }

  /**
   * Filter resources where column matches ANY of the provided regex patterns.
   * Generates: column.match(any,pattern1,pattern2,...)
   */
  matchAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "matchAny",
      value: patterns,
    });
  }

  /**
   * Filter resources where column matches ALL of the provided regex patterns.
   * Generates: column.match(all,pattern1,pattern2,...)
   */
  matchAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "matchAll",
      value: patterns,
    });
  }

  /**
   * Filter resources where column matches ANY of the provided regex patterns (case-insensitive).
   * Generates: column.imatch(any,pattern1,pattern2,...)
   */
  imatchAny<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "imatchAny",
      value: patterns,
    });
  }

  /**
   * Filter resources where column matches ALL of the provided regex patterns (case-insensitive).
   * Generates: column.imatch(all,pattern1,pattern2,...)
   */
  imatchAll<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    ...patterns: string[]
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "imatchAll",
      value: patterns,
    });
  }

  // ============ ARRAY/LIST OPERATORS ============

  /**
   * Filter resources where attribute is NOT in the list of values.
   *
   * @param column - The column to filter on
   * @param values - The list of values to exclude
   */
  notin<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    values: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>[],
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "notin",
      value: values,
    });
  }

  /**
   * Filter resources where arrays overlap (have elements in common).
   * Uses PostgreSQL && operator.
   *
   * @param column - The column to filter on
   * @param values - The array values to check for overlap
   */
  ov<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    values: ColumnValueOrReference<any[], TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "ov",
      value: values,
    });
  }

  /**
   * Filter resources where the column array/jsonb contains the specified value(s).
   * Uses PostgreSQL @> operator.
   *
   * @param column - The column to filter on
   * @param value - The value(s) that should be contained
   */
  cs<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "cs",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  /**
   * Filter resources where the column array/jsonb is contained by the specified value(s).
   * Uses PostgreSQL <@ operator.
   *
   * @param column - The column to filter on
   * @param value - The value(s) that should contain the column
   */
  cd<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "cd",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  // ============ RANGE OPERATORS ============

  /**
   * Filter resources where the range is strictly left of another range.
   * Uses PostgreSQL << operator.
   *
   * @param column - The column to filter on
   * @param range1 - The first range value
   * @param range2 - The second range value
   */
  sl<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    range1: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
    range2: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "sl",
      value: [range1, range2],
    });
  }

  /**
   * Filter resources where the range is strictly right of another range.
   * Uses PostgreSQL >> operator.
   *
   * @param column - The column to filter on
   * @param range1 - The first range value
   * @param range2 - The second range value
   */
  sr<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    range1: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
    range2: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "sr",
      value: [range1, range2],
    });
  }

  /**
   * Filter resources where the range does not extend to the right of another range.
   * Uses PostgreSQL &< operator.
   *
   * @param column - The column to filter on
   * @param range1 - The first range value
   * @param range2 - The second range value
   */
  nxr<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    range1: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
    range2: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "nxr",
      value: [range1, range2],
    });
  }

  /**
   * Filter resources where the range does not extend to the left of another range.
   * Uses PostgreSQL &> operator.
   *
   * @param column - The column to filter on
   * @param range1 - The first range value
   * @param range2 - The second range value
   */
  nxl<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    range1: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
    range2: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "nxl",
      value: [range1, range2],
    });
  }

  /**
   * Filter resources where the ranges are adjacent.
   * Uses PostgreSQL -|- operator.
   *
   * @param column - The column to filter on
   * @param range1 - The first range value
   * @param range2 - The second range value
   */
  adj<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    range1: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
    range2: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "adj",
      value: [range1, range2],
    });
  }

  // ============ NULL OPERATORS ============

  /**
   * Filter resources where the column IS NULL.
   *
   * @param column - The column to filter on
   */
  null<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "null",
      value: null,
    });
  }

  /**
   * Filter resources where the column IS NOT NULL.
   *
   * @param column - The column to filter on
   */
  notnull<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "notnull",
      value: null,
    });
  }

  /**
   * Filter resources where the column IS DISTINCT FROM the value.
   * This is different from neq because it handles NULL differently.
   *
   * @param column - The column to filter on
   * @param value - Optional: The value to check distinctness against
   */
  isdistinct<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value?: ColumnValueOrReference<
      ResolveColumnType<TTable, TJoinedTables, K & string>,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "isdistinct",
      value,
      isColumnReference:
        value !== undefined ? this.isColumnReference(value) : false,
    });
  }

  // ============ FULL-TEXT SEARCH OPERATORS ============

  /**
   * Full-text search using to_tsquery.
   * Uses PostgreSQL @@ operator with to_tsquery.
   *
   * @param column - The column to search in (must have a text search index)
   * @param queryOrLang - The tsquery string or language code
   * @param query - Optional: The tsquery string if first param is language
   */
  fts<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    queryOrLang: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
    query?: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "fts",
      value: query !== undefined ? [queryOrLang, query] : queryOrLang,
    });
  }

  /**
   * Full-text search using plainto_tsquery.
   * Uses PostgreSQL @@ operator with plainto_tsquery.
   *
   * @param column - The column to search in (must have a text search index)
   * @param queryOrLang - The plain text query string or language code
   * @param query - Optional: The plain text query string if first param is language
   */
  plfts<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    queryOrLang: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
    query?: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "plfts",
      value: query !== undefined ? [queryOrLang, query] : queryOrLang,
    });
  }

  /**
   * Full-text search using phraseto_tsquery.
   * Uses PostgreSQL @@ operator with phraseto_tsquery.
   *
   * @param column - The column to search in (must have a text search index)
   * @param queryOrLang - The phrase query string or language code
   * @param query - Optional: The phrase query string if first param is language
   */
  phfts<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    queryOrLang: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
    query?: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "phfts",
      value: query !== undefined ? [queryOrLang, query] : queryOrLang,
    });
  }

  /**
   * Full-text search using websearch_to_tsquery.
   * Uses PostgreSQL @@ operator with websearch_to_tsquery.
   *
   * @param column - The column to search in (must have a text search index)
   * @param queryOrLang - The web search query string or language code
   * @param query - Optional: The web search query string if first param is language
   */
  wfts<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    queryOrLang: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
    query?: ColumnValueOrReference<
      string,
      TSchema,
      TParentTable,
      TJoinedTables
    >,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "wfts",
      value: query !== undefined ? [queryOrLang, query] : queryOrLang,
    });
  }

  // ============ MISC OPERATORS ============

  /**
   * Filter resources where ALL values in the array satisfy the condition.
   * Uses PostgreSQL ALL operator.
   *
   * @param column - The column to filter on
   * @param value - The value to compare with all elements
   */
  all<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "all",
      value,
      isColumnReference: this.isColumnReference(value),
    });
  }

  /**
   * Filter resources where ANY value in the array satisfies the condition.
   * Uses PostgreSQL ANY operator.
   *
   * @param column - The column to filter on
   * @param value - The value to compare with any element
   */
  any<K extends AllAvailableColumns<TTable, never> | JsonPath<TTable>>(
    column: K,
    value: ColumnValueOrReference<any, TSchema, TParentTable, TJoinedTables>,
  ): this {
    return this.addCondition({
      column: column as string,
      operator: "any",
      value,
      isColumnReference: this.isColumnReference(value),
    });
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

  /**
   * Sets the maximum number of records to retrieve.
   *
   * @param limit - The maximum number of records to limit the query to.
   * @returns The current instance of the builder for method chaining.
   */
  limit(limit: number) {
    this._extra.limit = limit;
    return this;
  }

  /**
   * Sets the number of records to skip before starting to collect the result set.
   *
   * @param offset - The number of records to skip.
   * @returns The current instance of the builder for method chaining.
   */
  offset(offset: number) {
    this._extra.offset = offset;
    return this;
  }

  /**
   * Adds grouping instructions to the table query.
   *
   * @param columns - An array of column names to group by.
   * @returns The current instance of the builder for method chaining.
   */
  groupBy(
    ...columns: (
      | AllAvailableGroupColumns<TTable, TJoinedTables>
      | JsonPath<TTable, TJoinedTables>
    )[]
  ) {
    const _groupBy = columns.map((col) => col.toString());
    this._extra.groupBy = Array.from(
      new Set([...(this._extra.groupBy ?? []), ..._groupBy]),
    );
    return this;
  }

  /**
   * Sets a range for the query results.
   *
   * @param start - The starting index of the range (inclusive).
   * @param end - The ending index of the range (inclusive).
   * @returns The current instance of the builder for method chaining.
   */
  range(start: number, end: number) {
    if (start < 0 || end < 0 || start > end) {
      throw new Error(
        "Invalid range: 'start' must be less than or equal to 'end', and both must be non-negative.",
      );
    }
    this._extra.limit = end - start + 1; // Adjust limit to include both start and end
    this._extra.offset = start; // Set offset to start
    return this;
  }

  /**
   * Adds ordering instructions to the table query.
   *
   * @param orders - An object where the keys represent column names and the values
   *                 represent the order direction (`asc`, `desc`, `nullsfirst`, `nullslast`, `asc|desc.nullsfirst`, `asc|desc.nullslast`).
   *                 Example: `{ columnName: 'asc', anotherColumn: 'desc' }`.
   * @returns The current instance of the builder for method chaining.
   */
  orderBy(orders: OrderObject<TTable>) {
    const _orders: string[] = [];
    Object.entries(orders).forEach(([column, order]) => {
      _orders.push(`${column}.${order}`);
    });
    this._extra.orders = Array.from(
      new Set([...(this._extra.orders ?? []), ..._orders]),
    );
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

  // ============ QUERY EXECUTION ============

  protected async execute() {
    return this._client.withSafeResponse(async () => {
      if (this._single && this._maybeSingle) {
        throw new Error("Cannot use both single() and maybeSingle() together.");
      }

      // Apply limit(1) automatically when using single or maybeSingle
      if ((this._single || this._maybeSingle) && !this._extra.limit) {
        this._extra.limit = 1;
      }

      const query = new URLSearchParams(this.toString());
      const url = new URL(
        `${this._client.config.endpoint}/schemas/${this._config.schema}/tables/${this._config.tableName}`,
      );
      url.search = query.toString();

      try {
        const response = await this._client.call("GET", url);

        if (this._single) {
          if (!response || response.length === 0) {
            throw new Error("No result found");
          }
          return response[0];
        }

        if (this._maybeSingle) {
          return response?.[0] ?? null;
        }

        return response;
      } catch (error) {
        if (error instanceof NuvixException) {
          throw error;
        }
        throw new NuvixException(
          `Query execution failed: ${error instanceof Error ? error.message : String(error)}`,
          400,
          `QUERY_EXECUTION_ERROR`,
        );
      }
    });
  }

  single(): TableQueryBuilder<
    TClient,
    TTable,
    TSchema,
    TResult,
    TParentTable,
    TJoinedTables,
    TResult
  > {
    this._single = true;
    this._maybeSingle = false;
    return this as any;
  }

  maybeSingle(): TableQueryBuilder<
    TClient,
    TTable,
    TSchema,
    TResult,
    TParentTable,
    TJoinedTables,
    TResult | null
  > {
    this._single = false;
    this._maybeSingle = true;
    return this as any;
  }

  then<TResult1 = ResponseType<TClient, TResultFinal>, TResult2 = never>(
    onfulfilled?:
      | ((
          value: ResponseType<TClient, TResultFinal>,
        ) => TResult1 | PromiseLike<TResult1>)
      | null
      | undefined,
    onrejected?:
      | ((reason: any) => TResult2 | PromiseLike<TResult2>)
      | null
      | undefined,
  ): PromiseLike<TResult1 | TResult2> {
    return this.execute().then(onfulfilled as any, onrejected);
  }

  // ============ QUERY STRING BUILDING ============

  /**
   * Converts the current table builder configuration into a NuvQL query string.
   *
   * @returns {string} The generated NuvQL query string based on the selected columns,
   * conditions, and joins.
   *
   * The method constructs the query by:
   * - Selecting columns (`*` if none are specified).
   * - Applying conditions using the `buildCondition` method.
   * - Including join queries if applicable.
   *
   * If the builder is configured as a join builder and join options are provided,
   * it generates a join query using `_buildJoinQuery`. Otherwise, it generates
   * the main query using `_buildMainQuery`.
   */
  toString(): string {
    const select =
      this._selectedColumns.length === 0
        ? "*"
        : this._selectedColumns.join(",");
    const filter = this._conditions
      .map((c) => this.buildCondition(c))
      .join(",");
    const joins = this._joins.map((j) => j.query).join(",");

    if (this._config.isJoinBuilder && this._config.joinOptions) {
      return this._buildJoinQuery(select, filter, joins);
    }

    return this._buildMainQuery(select, filter, joins);
  }

  private _buildJoinQuery(
    select: string,
    filter: string,
    joins: string,
  ): string {
    const { joinOptions, tableName } = this._config;
    const opts = joinOptions!;
    const type = `$.join(${opts.type ?? "inner"})`;
    const flatten = "flatten" in opts && opts.flatten ? "..." : "";
    const kind =
      !flatten && "kind" in opts && opts.kind ? `.${opts.kind ?? "many"}` : "";
    const alias = opts.as ? `${opts.as}:` : "";

    const conditions = [filter, type].filter(Boolean);
    let query = `${flatten}${alias}${tableName}${kind}`;
    let extra = this._buildExtraJoinQueryParams();

    query += `{${conditions.join(",")}${extra}}`;

    if (select !== "*" || joins) {
      const innerSelect = [select, joins].filter((s) => s !== "*").join(",");
      query += `(${innerSelect || "*"})`;
    }

    return query;
  }

  private _buildMainQuery(
    select: string,
    filter: string,
    joins: string,
  ): string {
    const finalSelect = [select, joins].filter(Boolean).join(",");
    let query = `select=${finalSelect}`;
    if (filter) {
      query += `&filter=${filter}`;
    }
    query += this._buildExtraQueryParams();
    return query;
  }

  private _buildExtraQueryParams(): string {
    let extra = "";
    if (typeof this._extra.limit === "number") {
      extra += `&limit=${this._extra.limit}`;
    }
    if (this._extra?.orders?.length) {
      const orders = this._extra.orders.join(",");
      extra += `&order=${orders}`;
    }
    if (this._extra?.offset) {
      extra += `&offset=${this._extra.offset}`;
    }
    if (this._extra?.groupBy?.length) {
      const groupBy = this._extra.groupBy.join(",");
      extra += `&group=${groupBy}`;
    }
    return extra;
  }

  private _buildExtraJoinQueryParams(): string {
    let extra = "";
    if (typeof this._extra.limit === "number") {
      extra += `,$.limit(${this._extra.limit})`;
    }
    if (this._extra?.orders?.length) {
      const orders = this._extra.orders.join(",");
      extra += `,$.order(${orders})`;
    }
    if (this._extra?.offset) {
      extra += `,$.offset(${this._extra.offset})`;
    }
    if (this._extra?.groupBy?.length) {
      const groupBy = this._extra.groupBy.join(",");
      extra += `,$.group(${groupBy})`;
    }
    return extra;
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
        // Comparison operators with ANY quantifier
        case "eqAny":
          const eqAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.eq(any,${eqAnyValues})`;
        case "neqAny":
          const neqAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.neq(any,${neqAnyValues})`;
        case "gtAny":
          const gtAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.gt(any,${gtAnyValues})`;
        case "gteAny":
          const gteAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.gte(any,${gteAnyValues})`;
        case "ltAny":
          const ltAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.lt(any,${ltAnyValues})`;
        case "lteAny":
          const lteAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.lte(any,${lteAnyValues})`;
        // Comparison operators with ALL quantifier
        case "eqAll":
          const eqAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.eq(all,${eqAllValues})`;
        case "neqAll":
          const neqAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.neq(all,${neqAllValues})`;
        case "gtAll":
          const gtAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.gt(all,${gtAllValues})`;
        case "gteAll":
          const gteAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.gte(all,${gteAllValues})`;
        case "ltAll":
          const ltAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.lt(all,${ltAllValues})`;
        case "lteAll":
          const lteAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.lte(all,${lteAllValues})`;
        case "like":
          return `${column}.like(${this.formatValue(value)})`;
        case "ilike":
          return `${column}.ilike(${this.formatValue(value)})`;
        case "match":
          return `${column}.match(${this.formatValue(value)})`;
        case "imatch":
          return `${column}.imatch(${this.formatValue(value)})`;
        // String operators with ANY quantifier
        case "likeAny":
          const likeAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.like(any,${likeAnyValues})`;
        case "ilikeAny":
          const ilikeAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.ilike(any,${ilikeAnyValues})`;
        case "matchAny":
          const matchAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.match(any,${matchAnyValues})`;
        case "imatchAny":
          const imatchAnyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.imatch(any,${imatchAnyValues})`;
        // String operators with ALL quantifier
        case "likeAll":
          const likeAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.like(all,${likeAllValues})`;
        case "ilikeAll":
          const ilikeAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.ilike(all,${ilikeAllValues})`;
        case "matchAll":
          const matchAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.match(all,${matchAllValues})`;
        case "imatchAll":
          const imatchAllValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.imatch(all,${imatchAllValues})`;
        case "in":
          const inValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.in(${inValues})`;
        case "notin":
          const ninValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.notin(${ninValues})`;
        case "ov":
          const ovValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.ov(${ovValues})`;
        case "cs":
          const csValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.cs(${csValues})`;
        case "cd":
          const cdValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.cd(${cdValues})`;
        case "between":
          const [min, max] = Array.isArray(value) ? value : [value, value];
          return `${column}.between(${this.formatValue(min)},${this.formatValue(max)})`;
        case "nbetween":
          const [nMin, nMax] = Array.isArray(value) ? value : [value, value];
          return `${column}.nbetween(${this.formatValue(nMin)},${this.formatValue(nMax)})`;
        case "sl":
          const [slMin, slMax] = Array.isArray(value) ? value : [value, value];
          return `${column}.sl(${this.formatValue(slMin)},${this.formatValue(slMax)})`;
        case "sr":
          const [srMin, srMax] = Array.isArray(value) ? value : [value, value];
          return `${column}.sr(${this.formatValue(srMin)},${this.formatValue(srMax)})`;
        case "nxr":
          const [nxrMin, nxrMax] = Array.isArray(value)
            ? value
            : [value, value];
          return `${column}.nxr(${this.formatValue(nxrMin)},${this.formatValue(nxrMax)})`;
        case "nxl":
          const [nxlMin, nxlMax] = Array.isArray(value)
            ? value
            : [value, value];
          return `${column}.nxl(${this.formatValue(nxlMin)},${this.formatValue(nxlMax)})`;
        case "adj":
          const [adjMin, adjMax] = Array.isArray(value)
            ? value
            : [value, value];
          return `${column}.adj(${this.formatValue(adjMin)},${this.formatValue(adjMax)})`;
        case "is":
          return `${column}.is(${this.formatSpecialValue(value)})`;
        case "isnot":
          return `${column}.isnot(${this.formatSpecialValue(value)})`;
        case "null":
          return `${column}.null()`;
        case "notnull":
          return `${column}.notnull()`;
        case "isdistinct":
          return value !== undefined
            ? `${column}.isdistinct(${this.formatValue(value)})`
            : `${column}.isdistinct()`;
        case "fts":
          const ftsArgs = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.fts(${ftsArgs})`;
        case "plfts":
          const plftsArgs = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.plfts(${plftsArgs})`;
        case "phfts":
          const phftsArgs = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.phfts(${phftsArgs})`;
        case "wfts":
          const wftsArgs = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.wfts(${wftsArgs})`;
        case "all":
          const allValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.all(${allValues})`;
        case "any":
          const anyValues = Array.isArray(value)
            ? value.map((v) => this.formatValue(v)).join(",")
            : this.formatValue(value);
          return `${column}.any(${anyValues})`;
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
