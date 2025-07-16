import type { Client } from "../client";
import { DatabaseTypes } from "./types";

// ============ CORE TYPES ============

export type NuvqlOperator =
    | 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte'
    | 'like' | 'ilike' | 'in' | 'nin'
    | 'is' | 'isnot' | 'between' | 'nbetween'
    | 'contains' | 'startswith' | 'endswith';

export type NuvqlLogicalOperator = 'and' | 'or' | 'not';

export interface NuvqlFilterCondition {
    column: string | unknown;
    operator: NuvqlOperator;
    value: any;
    isColumnReference?: boolean;
}

export interface NuvqlLogicalCondition {
    operator: NuvqlLogicalOperator;
    conditions: NuvqlCondition[];
}

export type NuvqlCondition = NuvqlFilterCondition | NuvqlLogicalCondition;

// ============ COLUMN REFERENCE TYPES ============

// Column reference for type-safe cross-column comparisons
export type ColumnReference<T extends DatabaseTypes.GenericTable, K extends TableColumns<T>> =
    `"${string & K}"`;

// For join references - allows referencing columns from other tables
export type JoinColumnReference<TSchema extends DatabaseTypes.GenericSchema> =
    `"${string}.${string}"`;

// Cast types for PostgreSQL casting
export type Cast =
    | 'text' | 'varchar' | 'char'
    | 'int' | 'integer' | 'bigint' | 'smallint'
    | 'float' | 'real' | 'double' | 'numeric' | 'decimal'
    | 'boolean' | 'bool'
    | 'date' | 'time' | 'timestamp' | 'timestamptz'
    | 'json' | 'jsonb'
    | 'uuid'
    | string; // Allow custom casts

// ============ COLUMN BUILDER ============

export class ColumnBuilder<
    TColumn extends string = string,
    TAlias extends string | unknown = unknown,
    TCast extends Cast | unknown = unknown
> {
    private column: TColumn;
    private alias?: TAlias;
    private castType?: TCast;

    constructor(column: TColumn) {
        this.column = column;
    }

    as<A extends string>(alias: A): ColumnBuilder<TColumn, A, TCast> {
        const builder = new ColumnBuilder(this.column) as any;
        builder.alias = alias;
        builder.castType = this.castType;
        return builder;
    }

    cast<C extends Cast>(castType: C): ColumnBuilder<TColumn, TAlias, C> {
        const builder = new ColumnBuilder(this.column) as any;
        builder.alias = this.alias;
        builder.castType = castType;
        return builder;
    }

    toString(): string {
        let result = this.column;
        if (this.alias && typeof this.alias === 'string') {
            result = `${this.alias}:${result}` as any;
        }
        if (this.castType && typeof this.castType === 'string') {
            result = `${result}::${this.castType}` as any;
        }
        return result;
    }

    // Internal getters for type extraction
    getColumn(): TColumn { return this.column; }
    getAlias(): TAlias { return this.alias as TAlias; }
    getCast(): TCast { return this.castType as TCast; }
}

// Helper function to create column builders
export function column<T extends string>(name: T): ColumnBuilder<T> {
    return new ColumnBuilder(name);
}

// Helper function to create JSON path selections
export function jsonPath<
    TTable extends DatabaseTypes.GenericTable,
    TColumn extends string & TableColumns<TTable>,
    TPath extends string
>(
    column: TColumn,
    path: TPath
): `${TColumn}->${TPath}` {
    return `${column}->${path}` as const;
}

// Helper function to create aliased JSON path selections
export function aliasedJsonPath<
    TTable extends DatabaseTypes.GenericTable,
    TColumn extends string & TableColumns<TTable>,
    TPath extends string,
    TAlias extends string
>(
    alias: TAlias,
    column: TColumn,
    path: TPath
): `${TAlias}:${TColumn}->${TPath}` {
    return `${alias}:${column}->${path}` as const;
}

// ============ TYPE UTILITIES ============

// Extract column names from table type
type TableColumns<T extends DatabaseTypes.GenericTable> = keyof T['Row'];

// Check if a type is a valid column
type IsValidColumn<T, K> = K extends keyof T ? true : false;

// Get column type from table
type ColumnType<T extends DatabaseTypes.GenericTable, K extends TableColumns<T>> = T['Row'][K];

// Value type validation for operators
type ValidOperatorValue<T, Op extends NuvqlOperator> =
    Op extends 'in' | 'nin' ? T[] :
    Op extends 'between' | 'nbetween' ? [T, T] :
    Op extends 'is' | 'isnot' ? T | null | 'null' | 'not_null' | boolean :
    Op extends 'like' | 'ilike' | 'contains' | 'startswith' | 'endswith' ? string :
    T;

// ============ SELECTION TYPES ============

// JSON path extraction types
type JsonPath<T extends string> = T extends `${infer Start}->${infer Rest}`
    ? Rest extends `${infer Next}->${infer Further}`
    ? `${Start}_${JsonPath<`${Next}->${Further}`>}`
    : `${Start}_${Rest}`
    : T;

// Convert JSON path to column name
type JsonPathToColumn<T extends string> = JsonPath<T>;

// Extract JSON value type (simplified - in real implementation would need deeper inference)
type JsonValueType<
    TTable extends DatabaseTypes.GenericTable,
    TColumn extends TableColumns<TTable>,
    TPath extends string
> = ColumnType<TTable, TColumn> extends Record<string, any>
    ? any // Would need deeper JSON type inference here
    : ColumnType<TTable, TColumn> extends string
    ? any // JSON stored as string
    : unknown;

// Enhanced column selection types  
type SelectionInput<TTable extends DatabaseTypes.GenericTable> =
    | TableColumns<TTable>                                           // Simple column: 'name'
    | `${string}:${string & TableColumns<TTable>}`                   // Aliased: 'full_name:name'
    | `${string & TableColumns<TTable>}::${Cast}`                    // Cast: 'age::text'
    | `${string}:${string & TableColumns<TTable>}::${Cast}`          // Alias + Cast: 'age_text:age::text'
    | `${string & TableColumns<TTable>}->${string}`                  // JSON path: 'metadata->name'
    | `${string}:${string & TableColumns<TTable>}->${string}`        // Alias + JSON: 'user_name:metadata->name'
    | ColumnBuilder<any, any, any>                                   // Column builder
    | Record<string, TableColumns<TTable> | `${string & TableColumns<TTable>}->${string}`>; // Object alias

// Raw column with alias and cast
type RawColumn<C extends string, A extends string | unknown = unknown> =
    A extends string
    ? `${A}:${C}` | `${A}:${C}::${Cast}`
    : `${C}::${Cast}`;

// Column alias mapping
type AliasMapping<T extends Record<string, any>> = {
    [K in keyof T as K extends string ? K : never]: T[K] extends string
    ? T[K] extends `${infer Col}->${infer Path}`
    ? any // JSON path result
    : T[K]
    : T[K];
};

// Enhanced selection type extraction
type ExtractSelectionType<
    T extends DatabaseTypes.GenericTable,
    Input
> = Input extends ColumnBuilder<infer C, infer A, any>
    ? A extends string
    ? { [K in A]: C extends TableColumns<T> ? ColumnType<T, C> : unknown }
    : C extends TableColumns<T>
    ? { [K in C]: ColumnType<T, C> }
    : unknown
    : Input extends Record<string, infer V>
    ? {
        [K in keyof Input as K extends string ? K : never]:
        Input[K] extends `${infer Col}->${infer Path}`
        ? Col extends TableColumns<T>
        ? JsonValueType<T, Col, Path>
        : unknown
        : Input[K] extends TableColumns<T>
        ? ColumnType<T, Input[K]>
        : unknown
    }
    : Input extends `${infer A}:${infer Rest}`
    ? Rest extends `${infer Col}->${infer Path}`
    ? Col extends TableColumns<T>
    ? { [K in A]: JsonValueType<T, Col, Path> }
    : unknown
    : Rest extends `${infer Col}::${Cast}`
    ? Col extends TableColumns<T>
    ? { [K in A]: ColumnType<T, Col> }
    : unknown
    : Rest extends TableColumns<T>
    ? { [K in A]: ColumnType<T, Rest> }
    : unknown
    : Input extends `${infer Col}->${infer Path}`
    ? Col extends TableColumns<T>
    ? { [K in JsonPathToColumn<`${Col}_${JsonPath<Path>}`>]: JsonValueType<T, Col, Path> }
    : unknown
    : Input extends `${infer C}::${Cast}`
    ? C extends TableColumns<T>
    ? { [K in C]: ColumnType<T, C> }
    : unknown
    : Input extends TableColumns<T>
    ? { [K in Input]: ColumnType<T, Input> }
    : unknown;

// Improved merge selections with proper intersection
type MergeSelections<T extends readonly Record<string, any>[]> =
    T extends readonly [infer First, ...infer Rest]
    ? First extends Record<string, any>
    ? Rest extends readonly Record<string, any>[]
    ? First & MergeSelections<Rest>
    : First
    : Rest extends readonly Record<string, any>[]
    ? MergeSelections<Rest>
    : {}
    : {};

// Helper for selection validation
type ValidateSelection<
    TTable extends DatabaseTypes.GenericTable,
    TSelection
> = TSelection extends TableColumns<TTable>
    ? TSelection
    : TSelection extends `${infer Col}->${string}`
    ? Col extends TableColumns<TTable>
    ? TSelection
    : `Error: Column '${Col}' does not exist`
    : TSelection extends `${string}:${infer Rest}`
    ? Rest extends TableColumns<TTable>
    ? TSelection
    : Rest extends `${infer Col}->${string}`
    ? Col extends TableColumns<TTable>
    ? TSelection
    : `Error: Column '${Col}' does not exist`
    : Rest extends `${infer Col}::${Cast}`
    ? Col extends TableColumns<TTable>
    ? TSelection
    : `Error: Column '${Col}' does not exist`
    : `Error: Invalid selection format`
    : TSelection extends ColumnBuilder<any, any, any>
    ? TSelection
    : TSelection extends Record<string, any>
    ? TSelection
    : `Error: Invalid selection type`;

// Helper type to convert tuple to merged object type
type TupleToIntersection<T extends readonly any[]> = {
    [K in keyof T]: T[K] extends Record<string, any> ? T[K] : never;
} extends readonly (infer U)[] ?
    U extends Record<string, any> ? U : never : never;

// ============ JOIN TYPES ============

type FlattenJoin = {
    flatten: true;
    type?: 'left' | 'inner' | 'right' | 'full';
};

type LiteralJoin = {
    type?: 'left' | 'inner';
    shape?: 'one' | 'many';
};

type JoinOptions<T extends string = string> = {
    table: T;
    as?: string;
} & (FlattenJoin | LiteralJoin);

// Extract joined table type
type ExtractJoinedType<
    Schema extends DatabaseTypes.GenericSchema,
    JoinOpts extends JoinOptions,
    SelectedType
> = JoinOpts extends { flatten: true }
    ? SelectedType  // Flattened joins merge into parent
    : JoinOpts extends { as: infer Alias }
    ? Alias extends string
    ? JoinOpts extends { shape: 'one' }
    ? { [K in Alias]: SelectedType }
    : { [K in Alias]: SelectedType[] }
    : JoinOpts extends { shape: 'one' }
    ? SelectedType
    : SelectedType[]
    : JoinOpts extends { shape: 'one' }
    ? SelectedType
    : SelectedType[];

// ============ FILTER CHAIN TYPES ============

// Base filter interface for method chaining
interface BaseFilter<
    TClient extends Client,
    TTable extends DatabaseTypes.GenericTable,
    TSchema extends DatabaseTypes.GenericSchema,
    TResult,
    TIsSubquery extends boolean = false
> {
    // Comparison operators
    eq<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    neq<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    gt<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    gte<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    lt<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    lte<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    // Text operators (only for string columns)
    like<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    ilike<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    contains<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    startswith<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    endswith<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    // Array operators
    in<K extends TableColumns<TTable>>(
        column: K,
        values: ColumnType<TTable, K>[]
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    nin<K extends TableColumns<TTable>>(
        column: K,
        values: ColumnType<TTable, K>[]
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    // Range operators
    between<K extends TableColumns<TTable>>(
        column: K,
        min: ColumnType<TTable, K>,
        max: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    nbetween<K extends TableColumns<TTable>>(
        column: K,
        min: ColumnType<TTable, K>,
        max: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    // Null operators
    is<K extends TableColumns<TTable>>(
        column: K,
        value: null | boolean | 'null' | 'not_null'
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    isnot<K extends TableColumns<TTable>>(
        column: K,
        value: null | boolean | 'null' | 'not_null'
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    // Convenience methods
    isNull<K extends TableColumns<TTable>>(
        column: K
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    isNotNull<K extends TableColumns<TTable>>(
        column: K
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    // Logical operators
    and(
        callback: (
            filter: QueryFilter<TClient, TTable, TSchema, TResult, true>
        ) => QueryFilter<TClient, TTable, TSchema, TResult, true>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    or(
        callback: (
            filter: QueryFilter<TClient, TTable, TSchema, TResult, true>
        ) => QueryFilter<TClient, TTable, TSchema, TResult, true>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    not(
        callback: (
            filter: QueryFilter<TClient, TTable, TSchema, TResult, true>
        ) => QueryFilter<TClient, TTable, TSchema, TResult, true>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

    // Internal method for retrieving conditions
    getConditions(): NuvqlCondition[];
}

// Query filter type (used in subqueries)
type QueryFilter<
    TClient extends Client,
    TTable extends DatabaseTypes.GenericTable,
    TSchema extends DatabaseTypes.GenericSchema,
    TResult,
    TIsSubquery extends boolean = false
> = TIsSubquery extends true
    ? BaseFilter<TClient, TTable, TSchema, TResult, TIsSubquery>
    : BaseFilter<TClient, TTable, TSchema, TResult, TIsSubquery> & {
        // Selection methods (only available on main query)
        select(): TableQueryBuilder<TClient, TTable, TSchema, TTable['Row']>;
        select(columns: '*'): TableQueryBuilder<TClient, TTable, TSchema, TTable['Row']>;
        select<TSelections extends readonly any[]>(
            ...columns: TSelections
        ): TableQueryBuilder<
            TClient,
            TTable,
            TSchema,
            MergeSelections<{
                readonly [K in keyof TSelections]: ExtractSelectionType<TTable, TSelections[K]>
            } & readonly Record<string, any>[]>
        >;

        // Join methods
        join<TJoinTable extends keyof TSchema['Tables']>(
            table: TJoinTable,
            callback: (
                builder: TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema>
            ) => QueryFilter<TClient, TSchema['Tables'][TJoinTable], TSchema, any, true>
        ): TableQueryBuilder<
            TClient,
            TTable,
            TSchema,
            TResult & ExtractJoinedType<TSchema, { table: string & TJoinTable }, any>
        >;

        join<TJoinTable extends keyof TSchema['Tables']>(
            options: JoinOptions<TJoinTable & string>,
            callback: (
                builder: TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema>
            ) => QueryFilter<TClient, TSchema['Tables'][TJoinTable], TSchema, any, true>
        ): TableQueryBuilder<
            TClient,
            TTable,
            TSchema,
            TResult & ExtractJoinedType<TSchema, JoinOptions<TJoinTable & string>, any>
        >;
    };

// Alias for better type inference
type TypedQueryFilter<
    TClient extends Client,
    TTable extends DatabaseTypes.GenericTable,
    TSchema extends DatabaseTypes.GenericSchema,
    TResult,
    TIsSubquery extends boolean = false
> = QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery>;

// ============ MAIN QUERY BUILDER ============

export class TableQueryBuilder<
    TClient extends Client,
    TTable extends DatabaseTypes.GenericTable,
    TSchema extends DatabaseTypes.GenericSchema,
    TResult = TTable['Row'],
    TIsSubquery extends boolean = false
> implements BaseFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
    private client: TClient;
    private tableName: string;
    private schema: string;
    private selectedColumns: string[] = [];
    private conditions: NuvqlCondition[] = [];
    private _isJoinBuilder: boolean = false;
    private _joins: string[] = [];
    private _joinOptions?: JoinOptions;

    constructor(
        client: TClient,
        config: {
            tableName: string;
            schema: string;
            isJoinBuilder?: boolean;
            joinOptions?: JoinOptions;
        }
    ) {
        this.client = client;
        this.tableName = config.tableName;
        this.schema = config.schema;
        this._isJoinBuilder = config.isJoinBuilder ?? false;
        this._joinOptions = config.joinOptions;
    }

    // ============ SELECTION METHODS ============

    select(): TableQueryBuilder<TClient, TTable, TSchema, TTable['Row']>;
    select(columns: '*'): TableQueryBuilder<TClient, TTable, TSchema, TTable['Row']>;
    select<TSelections extends readonly SelectionInput<TTable>[]>(
        ...columns: TSelections
    ): TableQueryBuilder<
        TClient,
        TTable,
        TSchema,
        MergeSelections<{
            readonly [K in keyof TSelections]: ExtractSelectionType<TTable, TSelections[K]>
        } & readonly Record<string, any>[]>
    >;
    select(...columns: any[]): TableQueryBuilder<TClient, TTable, TSchema, any> {
        // Clear previous selections
        this.selectedColumns = [];

        if (columns.length === 0 || (columns.length === 1 && columns[0] === '*')) {
            // Select all columns
            return this as any;
        }

        columns.forEach((column) => {
            if (column instanceof ColumnBuilder) {
                this.selectedColumns.push(column.toString());
            } else if (typeof column === 'object' && column !== null) {
                // Handle alias objects like { alias: 'column' }
                Object.entries(column).forEach(([alias, col]) => {
                    if (typeof col === 'string') {
                        // Check if it's a JSON path
                        if (col.includes('->')) {
                            // Convert JSON path: metadata->name becomes metadata_name if no alias
                            const processedCol = `${alias}:${col}`;
                            this.selectedColumns.push(processedCol);
                        } else {
                            this.selectedColumns.push(`${alias}:${col}`);
                        }
                    }
                });
            } else if (typeof column === 'string') {
                // Handle JSON paths: convert column->path to column_path if no alias
                if (column.includes('->') && !column.includes(':')) {
                    const [baseCol, ...pathParts] = column.split('->');
                    const flattenedName = `${baseCol}_${pathParts.join('_')}`;
                    this.selectedColumns.push(`${flattenedName}:${column}`);
                } else {
                    this.selectedColumns.push(column);
                }
            } else {
                throw new Error(
                    'Invalid column type. Expected string, ColumnBuilder, or alias object.'
                );
            }
        });

        return this as any;
    }

    // ============ JOIN METHODS ============

    join<TJoinTable extends keyof TSchema['Tables']>(
        table: TJoinTable,
        callback: (
            builder: TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema>
        ) => QueryFilter<TClient, TSchema['Tables'][TJoinTable], TSchema, any, true>
    ): TableQueryBuilder<TClient, TTable, TSchema, TResult>;

    join<TJoinTable extends keyof TSchema['Tables']>(
        options: JoinOptions<TJoinTable & string>,
        callback: (
            builder: TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema>
        ) => QueryFilter<TClient, TSchema['Tables'][TJoinTable], TSchema, any, true>
    ): TableQueryBuilder<TClient, TTable, TSchema, TResult>;

    join<TJoinTable extends keyof TSchema['Tables']>(
        tableOrOptions: TJoinTable | JoinOptions<TJoinTable & string>,
        callback: (
            builder: TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema>
        ) => QueryFilter<TClient, TSchema['Tables'][TJoinTable], TSchema, any, true>
    ): TableQueryBuilder<TClient, TTable, TSchema, TResult> {
        let table: TJoinTable;
        let options: JoinOptions<string>;

        if (typeof tableOrOptions === 'object') {
            table = tableOrOptions.table as TJoinTable;
            options = tableOrOptions;
        } else {
            table = tableOrOptions;
            options = { table: table as string };
        }

        const joinBuilder = new TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema>(
            this.client,
            {
                tableName: table as string,
                schema: this.schema,
                isJoinBuilder: true,
                joinOptions: options
            }
        );

        const joinQuery = callback(joinBuilder);
        const joinString = joinQuery.toString();
        this._joins.push(joinString);

        return this;
    }

    // ============ COMPARISON OPERATORS ============

    eq<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> | ColumnReference<TTable, K>
    ): TypedQueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'eq',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    neq<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> | ColumnReference<TTable, K>
    ): TypedQueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'neq',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    gt<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> | ColumnReference<TTable, K>
    ): TypedQueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'gt',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    gte<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> | ColumnReference<TTable, K>
    ): TypedQueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'gte',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    lt<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> | ColumnReference<TTable, K>
    ): TypedQueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'lt',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    lte<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> | ColumnReference<TTable, K>
    ): TypedQueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'lte',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    // ============ TEXT OPERATORS ============

    like<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'like',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    ilike<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'ilike',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    contains<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'contains',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    startswith<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'startswith',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    endswith<K extends TableColumns<TTable>>(
        column: K,
        value: ColumnType<TTable, K> extends string ? string : never
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'endswith',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    // ============ ARRAY OPERATORS ============

    in<K extends TableColumns<TTable>>(
        column: K,
        values: ColumnType<TTable, K>[]
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'in',
            value: values
        });
    }

    nin<K extends TableColumns<TTable>>(
        column: K,
        values: ColumnType<TTable, K>[]
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'nin',
            value: values
        });
    }

    // ============ RANGE OPERATORS ============

    between<K extends TableColumns<TTable>>(
        column: K,
        min: ColumnType<TTable, K>,
        max: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'between',
            value: [min, max]
        });
    }

    nbetween<K extends TableColumns<TTable>>(
        column: K,
        min: ColumnType<TTable, K>,
        max: ColumnType<TTable, K>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'nbetween',
            value: [min, max]
        });
    }

    // ============ NULL OPERATORS ============

    is<K extends TableColumns<TTable>>(
        column: K,
        value: null | boolean | 'null' | 'not_null'
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'is',
            value
        });
    }

    isnot<K extends TableColumns<TTable>>(
        column: K,
        value: null | boolean | 'null' | 'not_null'
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.addCondition({
            column,
            operator: 'isnot',
            value
        });
    }

    // ============ CONVENIENCE METHODS ============

    isNull<K extends TableColumns<TTable>>(
        column: K
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.is(column, 'null');
    }

    isNotNull<K extends TableColumns<TTable>>(
        column: K
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.is(column, 'not_null');
    }

    isEmptyString<K extends TableColumns<TTable>>(
        column: K
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.eq(column, '' as any);
    }

    isNotEmptyString<K extends TableColumns<TTable>>(
        column: K
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        return this.neq(column, '' as any);
    }

    // ============ LOGICAL OPERATORS ============

    and(
        callback: (
            filter: QueryFilter<TClient, TTable, TSchema, TResult, true>
        ) => QueryFilter<TClient, TTable, TSchema, TResult, true>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        const nestedFilter = new TableQueryBuilder(
            this.client,
            { tableName: this.tableName, schema: this.schema }
        );

        const result = callback(nestedFilter as any);
        const nestedConditions = result.getConditions();

        if (nestedConditions.length > 0) {
            const andCondition: NuvqlLogicalCondition = {
                operator: 'and',
                conditions: nestedConditions
            };
            this.conditions.push(andCondition);
        }

        return this as any;
    }

    or(
        callback: (
            filter: QueryFilter<TClient, TTable, TSchema, TResult, true>
        ) => QueryFilter<TClient, TTable, TSchema, TResult, true>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        const nestedFilter = new TableQueryBuilder(
            this.client,
            { tableName: this.tableName, schema: this.schema }
        );

        const result = callback(nestedFilter as any);
        const nestedConditions = result.getConditions();

        if (nestedConditions.length > 0) {
            const orCondition: NuvqlLogicalCondition = {
                operator: 'or',
                conditions: nestedConditions
            };
            this.conditions.push(orCondition);
        }

        return this as any;
    }

    not(
        callback: (
            filter: QueryFilter<TClient, TTable, TSchema, TResult, true>
        ) => QueryFilter<TClient, TTable, TSchema, TResult, true>
    ): QueryFilter<TClient, TTable, TSchema, TResult, TIsSubquery> {
        const nestedFilter = new TableQueryBuilder(
            this.client,
            { tableName: this.tableName, schema: this.schema }
        );

        const result = callback(nestedFilter as any);
        const nestedConditions = result.getConditions();

        if (nestedConditions.length > 0) {
            const notCondition: NuvqlLogicalCondition = {
                operator: 'not',
                conditions: nestedConditions
            };
            this.conditions.push(notCondition);
        }

        return this as any;
    }

    // ============ UTILITY METHODS ============

    /**
     * Get the result type (for type inference)
     */
    inferResult(): TResult {
        return {} as TResult;
    }

    /**
     * Add a condition to the query
     */
    private addCondition(condition: NuvqlFilterCondition): any {
        this.conditions.push(condition);
        return this;
    }

    /**
     * Check if a value is a column reference
     */
    private isColumnReference(value: any): boolean {
        return typeof value === 'string' && value.startsWith('"') && value.endsWith('"');
    }

    /**
     * Get all conditions for this query
     */
    getConditions(): NuvqlCondition[] {
        return [...this.conditions];
    }

    // Convert to NUVQL filter string
    toString(): string {
        let select: string;
        let filter: string;
        if (this.selectedColumns.length === 0) {
            select = '*'
        } else {
            select = this.selectedColumns.join(',')
        }

        if (this.conditions.length === 1) {
            filter = this.buildCondition(this.conditions[0]);
        }
        filter = this.conditions.map(condition => this.buildCondition(condition)).join(',');

        if (this._joins.length > 0) {
            const joins = this._joins.join(',');
            select += `,${joins}`;
        }

        // Fix: check if _joinOptions exists instead of _join
        if (this._joinOptions) {
            if (!filter) throw new Error('Filter is required in join.') // TODO: ------------

            let _type: string = this._joinOptions.type ?? 'left';
            const _flatten = ('flatten' in this._joinOptions && this._joinOptions.flatten) ? '...' : '';
            const _shape: string = !_flatten && 'shape' in this._joinOptions ? `.${this._joinOptions.shape ?? 'many'}` : ''
            const _alias = this._joinOptions.as ? `${this._joinOptions.as}:` : '';

            return `${_flatten}${_alias}${this.tableName}${_shape}{${filter},$.join(${_type})}(${select})`;
        }

        return `select=${select}&filter=${filter}`;
    }

    private buildCondition(condition: NuvqlCondition): string {
        if ('operator' in condition && ['and', 'or', 'not'].includes(condition.operator)) {
            return this.buildLogicalCondition(condition as NuvqlLogicalCondition);
        } else {
            return this.buildFilterCondition(condition as NuvqlFilterCondition);
        }
    }

    private buildLogicalCondition(condition: NuvqlLogicalCondition): string {
        const { operator, conditions } = condition;
        const built = conditions.map(c => this.buildCondition(c));

        switch (operator) {
            case 'and':
                return `and(${built.join(',')})`;
            case 'or':
                return `or(${built.join(',')})`;
            case 'not':
                return `not(${built.join(',')})`;
            default:
                return built.join(',');
        }
    }

    private buildFilterCondition(condition: NuvqlFilterCondition): string {
        const { column, operator, value, isColumnReference } = condition;

        switch (operator) {
            case 'eq':
                return `${column}.eq(${this.formatValue(value, isColumnReference)})`;
            case 'neq':
                return `${column}.neq(${this.formatValue(value, isColumnReference)})`;
            case 'gt':
                return `${column}.gt(${this.formatValue(value, isColumnReference)})`;
            case 'gte':
                return `${column}.gte(${this.formatValue(value, isColumnReference)})`;
            case 'lt':
                return `${column}.lt(${this.formatValue(value, isColumnReference)})`;
            case 'lte':
                return `${column}.lte(${this.formatValue(value, isColumnReference)})`;
            case 'like':
                return `${column}.like(${this.formatValue(value, isColumnReference)})`;
            case 'ilike':
                return `${column}.ilike(${this.formatValue(value, isColumnReference)})`;
            case 'contains':
                return `${column}.contains(${this.formatValue(value, isColumnReference)})`;
            case 'startswith':
                return `${column}.startswith(${this.formatValue(value, isColumnReference)})`;
            case 'endswith':
                return `${column}.endswith(${this.formatValue(value, isColumnReference)})`;
            case 'in':
                const inValues = Array.isArray(value)
                    ? value.map(v => this.formatValue(v)).join(',')
                    : this.formatValue(value);
                return `${column}.in[${inValues}]`;
            case 'nin':
                const ninValues = Array.isArray(value)
                    ? value.map(v => this.formatValue(v)).join(',')
                    : this.formatValue(value);
                return `${column}.nin[${ninValues}]`;
            case 'between':
                const [min, max] = Array.isArray(value) ? value : [value, value];
                return `${column}.between[${this.formatValue(min)},${this.formatValue(max)}]`;
            case 'nbetween':
                const [nMin, nMax] = Array.isArray(value) ? value : [value, value];
                return `${column}.nbetween[${this.formatValue(nMin)},${this.formatValue(nMax)}]`;
            case 'is':
                return `${column}.is(${this.formatSpecialValue(value)})`;
            case 'isnot':
                return `${column}.isnot(${this.formatSpecialValue(value)})`;
            default:
                return `${column}.eq(${this.formatValue(value, isColumnReference)})`;
        }
    }

    private formatValue(value: any, isColumnReference?: boolean): string {
        if (isColumnReference) {
            // Column references use double quotes
            return value;
        }

        if (value === null || value === undefined) {
            return 'null';
        }

        if (typeof value === 'string') {
            // String values use single quotes, escape existing single quotes
            return `'${value.replace(/'/g, "\\'")}'`;
        }

        if (typeof value === 'boolean') {
            return value ? 'true' : 'false';
        }

        if (typeof value === 'number') {
            return String(value);
        }

        if (value instanceof Date) {
            return `'${value.toISOString()}'`;
        }

        // For arrays and objects, stringify and quote
        return `'${JSON.stringify(value).replace(/'/g, "\\'")}'`;
    }

    private formatSpecialValue(value: any): string {
        if (value === null || value === 'null') {
            return 'null';
        }
        if (value === 'not_null') {
            return 'not_null';
        }
        if (typeof value === 'boolean') {
            return value ? 'true' : 'false';
        }
        return this.formatValue(value);
    }
}

interface Users {
    Row: { name: string, sduud: "gello", id: number, age: number, role: string, created_at: Date, blog_id: number };
    Insert: { name: string, sduud: "gello", id: number };
    Update: { name: string, sduud: "gello", id: number };
    Delete: { name: string, sduud: "gello", id: number }
    Relationships: []
}
// Example usage:
const queryBuilder = new TableQueryBuilder<any, Users, any>({} as Client, { tableName: 'users', schema: 'public' });

const res = queryBuilder
    .select(
        'id', 'name', 'age:age::text', 'name->suu',
        { io: "name", uu: "sduud", kk: "id" },
        column('name').as('rr').cast('text'),
        column('id').as('$id'),
    )
    .join({ table: "blogs", as: 'user_blogs', flatten: true, type: 'full' }, $ => $.select('*').eq('id', '"blog_id"'))
    .join("comments", $ => $.select('*').eq('id', '"blog_id"'))
    .eq('sduud', 'gello')
    .or($ =>
        $.gt('age', 18)
            .ilike('name', '%john%')
    )
    .not($ =>
        $.in('role', ['admin', 'user'])
    )
    .gt('age', 18)
    .ilike('name', '%john%')
    .in('role', ['admin', 'user'])
    .between('created_at', new Date('2023-01-01'), new Date('2023-12-31'))
    .toString();


console.log(res); // For debugging, you can implement a toString method to see the query structure
