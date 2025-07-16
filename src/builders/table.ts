import type { Client } from "../client";
import { DatabaseTypes } from "./types";
import { NuvixException } from "../error";

// ============ CORE TYPES ============

export type NuvqlOperator =
    | 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte'
    | 'like' | 'ilike' | 'in' | 'nin'
    | 'is' | 'isnot' | 'between' | 'nbetween'
    | 'contains' | 'startswith' | 'endswith';

export type NuvqlLogicalOperator = 'and' | 'or' | 'not';

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
    readonly __brand: 'QueryBuildError';
};

// ============ TYPE UTILITIES ============

// Helper to check if a string is a valid column path
type IsValidColumnPath<T extends string> =
    T extends `${string}.${string}` ? true : false;

// Extract table name from column path
type ExtractTableName<T extends string> =
    T extends `${infer TableName}.${string}` ? TableName : never;

// Extract column name from column path  
type ExtractColumnName<T extends string> =
    T extends `${string}.${infer ColumnName}` ? ColumnName : T;

// Helper to validate join column references
type ValidateJoinColumnReference<
    TJoinedTables extends Record<string, DatabaseTypes.GenericTable>,
    TColumn extends string
> = IsValidColumnPath<TColumn> extends true
    ? ExtractTableName<TColumn> extends keyof TJoinedTables
    ? ExtractColumnName<TColumn> extends TableColumns<TJoinedTables[ExtractTableName<TColumn>]>
    ? TColumn
    : never
    : never
    : never;

// ============ UTILITY TYPES ============

type TableColumns<T extends DatabaseTypes.GenericTable> = string & keyof T['Row'];
type ColumnType<T extends DatabaseTypes.GenericTable, K extends TableColumns<T>> = T['Row'][K];

// Improved column value or reference type with better support for joined tables
type ColumnValueOrReference<
    T extends DatabaseTypes.GenericTable,
    K extends TableColumns<T>,
    TSchema extends DatabaseTypes.GenericSchema,
    TParentTable extends DatabaseTypes.GenericTable = T,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }> = {}
> =
    | ColumnType<T, K>
    | `"${TableColumns<T>}"`
    | (TParentTable extends DatabaseTypes.GenericTable
        ? `"${TableColumns<TParentTable>}"`
        : never)
    | (TSchema extends DatabaseTypes.GenericSchema
        ? { [TableName in keyof TSchema['Tables']]: `"${string & TableName}.${string & keyof TSchema['Tables'][TableName]['Row']}"` }[keyof TSchema['Tables']]
        : never)
    | (TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }>
        ? { [JoinName in keyof TJoinedTables]: `"${string & JoinName}.${string & keyof TJoinedTables[JoinName]['table']['Row']}"` }[keyof TJoinedTables]
        : never);

// Helper type for getting all available columns from current table + joined tables
type AllAvailableColumns<
    TTable extends DatabaseTypes.GenericTable,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }> = {}
> = TableColumns<TTable> | {
    [K in keyof TJoinedTables]: `${string & K}.${string & keyof TJoinedTables[K]['table']['Row']}`
}[keyof TJoinedTables];

// ============ COLUMN BUILDER ============

export type Cast =
    | 'text' | 'varchar' | 'char'
    | 'int' | 'integer' | 'bigint' | 'smallint'
    | 'float' | 'real' | 'double' | 'numeric' | 'decimal'
    | 'boolean' | 'bool'
    | 'date' | 'time' | 'timestamp' | 'timestamptz'
    | 'json' | 'jsonb'
    | 'uuid'
    | string;

export type ValidateCast<TColumnType, TCast extends Cast> =
    TCast extends 'text' | 'varchar' | 'char' ? string
    : TCast extends 'int' | 'integer' | 'bigint' | 'smallint' | 'float' | 'real' | 'double' | 'numeric' | 'decimal' ? number
    : TCast extends 'boolean' | 'bool' ? boolean
    : TCast extends 'date' | 'time' | 'timestamp' | 'timestamptz' ? Date | string
    : TCast extends 'json' | 'jsonb' ? object | string
    : TCast extends 'uuid' ? string
    : TColumnType;

export class ColumnBuilder<
    TColumn extends string = string,
    TAlias extends string | unknown = unknown,
    TCast extends Cast | unknown = unknown,
    TColumnType = unknown
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
        } = {}
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

    as<A extends string>(alias: A): ColumnBuilder<TColumn, A, TCast, TColumnType> {
        this._validateNotFrozen();
        return new ColumnBuilder(this._column, {
            alias,
            castType: this._castType,
            columnType: this._columnType,
            frozen: false,
        });
    }

    cast<C extends Cast>(castType: C): ColumnBuilder<TColumn, TAlias, C, ValidateCast<TColumnType, C>> {
        this._validateNotFrozen();
        this._validateCastCompatibility(castType);
        return new ColumnBuilder(this._column, {
            alias: this._alias,
            castType,
            columnType: {} as ValidateCast<TColumnType, C>,
            frozen: true,
        });
    }

    toString(): string {
        let result = this._column as string;
        if (this._alias && typeof this._alias === 'string') {
            result = `${result} as "${this._alias}"`;
        }
        if (this._castType && typeof this._castType === 'string') {
            result = `${result}::${this._castType}`;
        }
        return result;
    }

    getResultType(): TCast extends Cast ? ValidateCast<TColumnType, TCast> : TColumnType {
        return {} as any;
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
                'Cannot modify a frozen ColumnBuilder. Use the returned instance from method calls.',
                400,
                'FROZEN_COLUMN_BUILDER'
            );
        }
    }

    private _validateCastCompatibility(castType: Cast): void {
        if (!castType || typeof castType !== 'string') {
            throw new NuvixException(`Invalid cast type: ${castType}`, 400, 'INVALID_CAST_TYPE');
        }
    }
}

export function column<
    TTable extends DatabaseTypes.GenericTable,
    TColumn extends string & TableColumns<TTable>
>(name: TColumn): ColumnBuilder<TColumn, unknown, unknown, ColumnType<TTable, TColumn>> {
    return new ColumnBuilder(name, {
        columnType: {} as ColumnType<TTable, TColumn>,
    });
}

// Helper function for creating column references from joined tables
export function joinColumn<
    TJoinedTables extends Record<string, DatabaseTypes.GenericTable>,
    TJoinName extends keyof TJoinedTables,
    TColumn extends TableColumns<TJoinedTables[TJoinName]>
>(
    joinName: TJoinName,
    columnName: TColumn
): ColumnBuilder<`${string & TJoinName}.${string & TColumn}`, unknown, unknown, ColumnType<TJoinedTables[TJoinName], TColumn>> {
    const fullName = `${String(joinName)}.${String(columnName)}` as `${string & TJoinName}.${string & TColumn}`;
    return new ColumnBuilder(fullName, {
        columnType: {} as ColumnType<TJoinedTables[TJoinName], TColumn>,
    });
}

// ============ SELECTION TYPES ============

// Improved selection input type with better support for complex selections
type SelectionInput<
    T extends DatabaseTypes.GenericTable,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }> = {}
> =
    | TableColumns<T>
    | AllAvailableColumns<T, TJoinedTables>
    | `${string}:${TableColumns<T>}`
    | `${string}:${AllAvailableColumns<T, TJoinedTables>}`
    | `${TableColumns<T>}::${string}`
    | `${AllAvailableColumns<T, TJoinedTables>}::${string}`
    | `${string}:${TableColumns<T>}::${string}`
    | `${string}:${AllAvailableColumns<T, TJoinedTables>}::${string}`
    | `${TableColumns<T>}->${string}`
    | `${TableColumns<T>}->>${string}`
    | `${AllAvailableColumns<T, TJoinedTables>}->${string}`
    | `${AllAvailableColumns<T, TJoinedTables>}->>${string}`
    | `${string}:${TableColumns<T>}->${string}`
    | `${string}:${TableColumns<T>}->>${string}`
    | `${string}:${AllAvailableColumns<T, TJoinedTables>}->${string}`
    | `${string}:${AllAvailableColumns<T, TJoinedTables>}->>${string}`
    | ColumnBuilder<any, any, any, any>
    | Record<string, TableColumns<T> | AllAvailableColumns<T, TJoinedTables> | `${TableColumns<T>}->${string}` | `${TableColumns<T>}->>${string}` | `${AllAvailableColumns<T, TJoinedTables>}->${string}` | `${AllAvailableColumns<T, TJoinedTables>}->>${string}`>;

type JsonPath<
    T extends DatabaseTypes.GenericTable,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }> = {}
> =
    | `${TableColumns<T>}->${string}`
    | `${TableColumns<T>}->>${string}`
    | `${AllAvailableColumns<T, TJoinedTables>}->${string}`
    | `${AllAvailableColumns<T, TJoinedTables>}->>${string}`;

// Better JSON path to field name conversion - Fixed to handle >> correctly
export type JsonPathToFieldName<T extends string> =
    T extends `${infer Column}->>${infer Path}`
    ? `${Column}_${Path}`
    : T extends `${infer Column}->${infer Path}`
    ? `${Column}_${Path}`
    : T extends `${infer First}.${infer Rest}->>${infer Path}`
    ? `${First}_${Rest}_${Path}`
    : T extends `${infer First}.${infer Rest}->${infer Path}`
    ? `${First}_${Rest}_${Path}`
    : T;

// Helper to clean path segments (remove extra > characters) - Not needed anymore
type CleanPath<T extends string> = T;

// Helper to resolve column type from joined tables
type ResolveColumnType<
    T extends DatabaseTypes.GenericTable,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }>,
    Col extends string
> = Col extends TableColumns<T>
    ? ColumnType<T, Col>
    : Col extends `${infer JoinName}.${infer JoinCol}`
    ? JoinName extends keyof TJoinedTables
    ? JoinCol extends TableColumns<TJoinedTables[JoinName]['table']>
    ? ColumnType<TJoinedTables[JoinName]['table'], JoinCol>
    : any
    : any
    : any;

// Debug type for checking JSON path parsing
type TestJsonPath = JsonPathToFieldName<'comment->>oo'>; // Should be 'comment_oo'

// Simplified selection type extraction - focus on working correctly
type ExtractSelectionType<
    T extends DatabaseTypes.GenericTable,
    Input,
    TSchema extends DatabaseTypes.GenericSchema,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }> = {}
> =
    Input extends ColumnBuilder<infer C, infer A, infer Cast, infer ColType>
    ? A extends string
        ? { readonly [K in A]: Cast extends string ? ValidateCast<ColType, Cast> : ColType }
        : { readonly [K in C]: Cast extends string ? ValidateCast<ColType, Cast> : ColType }
    : Input extends `${infer Column}->>${infer Path}`
        ? { readonly [K in JsonPathToFieldName<Input>]: any }
    : Input extends `${infer Column}->${infer Path}`
        ? { readonly [K in JsonPathToFieldName<Input>]: any }
    : Input extends `${infer Alias}:${infer Col}`
        ? Col extends TableColumns<T>
            ? { readonly [K in Alias]: ColumnType<T, Col> }
            : { readonly [K in Alias]: any }
    : Input extends TableColumns<T>
        ? { readonly [K in Input]: ColumnType<T, Input> }
    : Input extends string
        ? { readonly [K in Input]: any } // Fallback for any string
    : never;

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

// Simplified combined result type 
type CombinedResultType<
    TResult,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }>
> = TResult & JoinedTablesResult<TJoinedTables> & FlattenedJoinProperties<TJoinedTables>;

// ============ JOIN TYPES ============

type JoinType = 'left' | 'inner' | 'right' | 'full';

interface FlattenJoin {
    flatten: true;
    type: JoinType;
}

interface ShapedJoin {
    type: JoinType;
    shape?: 'one' | 'many'; // Make shape optional with default 'many'
}

type JoinOptions<T extends string = string> = {
    table: T;
    as?: string;
} & (FlattenJoin | ShapedJoin);

// Improved combined schema that properly handles join relationships
type CombinedSchema<
    TTable extends DatabaseTypes.GenericTable,
    TSchema extends DatabaseTypes.GenericSchema,
    TJoinedTables extends Record<string, DatabaseTypes.GenericTable> = {}
> = DatabaseTypes.Prettify<
    TTable['Row'] & {
        [K in keyof TJoinedTables]: TJoinedTables[K]['Row'];
    }
>;

// Helper to determine the result type based on join options
type JoinResultType<
    TJoinTable extends DatabaseTypes.GenericTable,
    TJoinOptions extends JoinOptions
> = 'flatten' extends keyof TJoinOptions
    ? TJoinOptions['flatten'] extends true
    ? TJoinTable['Row'] // Flattened joins merge directly
    : never // If flatten is false, it should be shaped
    : 'shape' extends keyof TJoinOptions
    ? TJoinOptions['shape'] extends 'one'
    ? TJoinTable['Row'] | null // Single record or null
    : TJoinTable['Row'][] // Array of records (default)
    : TJoinTable['Row'][]; // Default to array

// Simplified joined tables result type
type JoinedTablesResult<
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }>
> = {
    [K in keyof TJoinedTables as TJoinedTables[K]['options'] extends { flatten: true }
        ? never
        : TJoinedTables[K]['options'] extends { as: infer Alias }
        ? Alias extends string
            ? Alias
            : K
        : K
    ]: TJoinedTables[K]['options'] extends { flatten: true }
        ? never
        : TJoinedTables[K]['options'] extends { shape: 'one' }
        ? TJoinedTables[K]['table']['Row'] | null
        : TJoinedTables[K]['table']['Row'][]
};

// Simplified flattened properties from joins
type FlattenedJoinProperties<
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }>
> = {
    [K in keyof TJoinedTables]: TJoinedTables[K]['options'] extends { flatten: true }
        ? TJoinedTables[K]['table']['Row']
        : {}
}[keyof TJoinedTables];

// Better joined tables tracking with options
type AddJoinedTableWithOptions<
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }>,
    TJoinName extends string,
    TJoinTable extends DatabaseTypes.GenericTable,
    TJoinOptions extends JoinOptions
> = TJoinedTables & {
    [K in TJoinName]: {
        table: TJoinTable;
        options: TJoinOptions;
    }
};

// ============ QUERY BUILDER ============

export class TableQueryBuilder<
    TClient extends Client,
    TTable extends DatabaseTypes.GenericTable,
    TSchema extends DatabaseTypes.GenericSchema,
    TResult = TTable['Row'],
    TParentTable extends DatabaseTypes.GenericTable = TTable,
    TJoinedTables extends Record<string, { table: DatabaseTypes.GenericTable; options: JoinOptions }> = {}
> {
    private selectedColumns: string[] = [];
    private conditions: NuvqlCondition[] = [];
    private joins: string[] = [];
    private joinedTables: TJoinedTables = {} as TJoinedTables;

    constructor(
        private client: TClient,
        private config: {
            tableName: string;
            schema: string;
            isJoinBuilder?: boolean;
            joinOptions?: JoinOptions;
            parentTableName?: string;
        }
    ) { }

    // Simplified select method overloads
    select(): TableQueryBuilder<TClient, TTable, TSchema, CombinedResultType<TTable['Row'], TJoinedTables>, TParentTable, TJoinedTables>;
    select(columns: '*'): TableQueryBuilder<TClient, TTable, TSchema, CombinedResultType<TTable['Row'], TJoinedTables>, TParentTable, TJoinedTables>;
    select<TSelections extends readonly string[]>(
        ...columns: TSelections
    ): TableQueryBuilder<
        TClient,
        TTable,
        TSchema,
        CombinedResultType<{
            [K in keyof TSelections as TSelections[K] extends string 
                ? TSelections[K] extends `${infer Col}->>${infer Path}`
                    ? JsonPathToFieldName<TSelections[K]>
                    : TSelections[K] extends `${infer Col}->${infer Path}`
                    ? JsonPathToFieldName<TSelections[K]>
                    : TSelections[K] extends TableColumns<TTable>
                    ? TSelections[K]
                    : TSelections[K]
                : never
            ]: TSelections[K] extends string
                ? TSelections[K] extends `${infer Col}->>${infer Path}` | `${infer Col}->${infer Path}`
                    ? any
                    : TSelections[K] extends TableColumns<TTable>
                    ? ColumnType<TTable, TSelections[K]>
                    : any
                : never
        }, TJoinedTables>,
        TParentTable,
        TJoinedTables
    >;
    select(...columns: any[]): any {
        this.selectedColumns = [];

        if (columns.length === 0 || (columns.length === 1 && columns[0] === '*')) {
            return this;
        }

        columns.forEach((column) => {
            if (column instanceof ColumnBuilder) {
                this.selectedColumns.push(column.toString());
            } else if (typeof column === 'object' && column !== null) {
                Object.entries(column).forEach(([alias, col]) => {
                    if (typeof col === 'string') {
                        this.selectedColumns.push(`${alias}:${col}`);
                    }
                });
            } else if (typeof column === 'string') {
                // Don't modify JSON path columns - keep them as-is
                this.selectedColumns.push(column);
            }
        });

        return this;
    }

    // Improved join methods with better type inference
    join<TJoinTable extends keyof TSchema['Tables']>(
        table: TJoinTable,
        callback: (builder: TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema, TSchema['Tables'][TJoinTable]['Row'], TTable>) => any
    ): TableQueryBuilder<
        TClient,
        TTable,
        TSchema,
        CombinedResultType<TResult, AddJoinedTableWithOptions<TJoinedTables, TJoinTable & string, TSchema['Tables'][TJoinTable], { table: TJoinTable & string; type: 'inner'; shape: 'many' }>>, // Update result type to include join
        TParentTable,
        AddJoinedTableWithOptions<TJoinedTables, TJoinTable & string, TSchema['Tables'][TJoinTable], { table: TJoinTable & string; type: 'inner'; shape: 'many' }>
    >;

    join<TJoinTable extends keyof TSchema['Tables']>(
        options: JoinOptions<TJoinTable & string>,
        callback: (builder: TableQueryBuilder<TClient, TSchema['Tables'][TJoinTable], TSchema, TSchema['Tables'][TJoinTable]['Row'], TTable>) => any
    ): TableQueryBuilder<
        TClient,
        TTable,
        TSchema,
        CombinedResultType<TResult, AddJoinedTableWithOptions<TJoinedTables, TJoinTable & string, TSchema['Tables'][TJoinTable], JoinOptions<TJoinTable & string>>>, // Update result type to include join
        TParentTable,
        AddJoinedTableWithOptions<TJoinedTables, TJoinTable & string, TSchema['Tables'][TJoinTable], JoinOptions<TJoinTable & string>>
    >;

    join(tableOrOptions: any, callback: any): any {
        let table: string;
        let options: JoinOptions;

        if (typeof tableOrOptions === 'object') {
            table = tableOrOptions.table;
            options = tableOrOptions;
        } else {
            table = tableOrOptions;
            options = { table, type: 'inner', shape: 'many' }; // Default options
        }

        const joinBuilder = new TableQueryBuilder(this.client, {
            tableName: table,
            schema: this.config.schema,
            isJoinBuilder: true,
            joinOptions: options,
            parentTableName: this.config.tableName,
        });

        // Call the callback to build the join condition
        const joinQuery = callback(joinBuilder);
        this.joins.push(joinQuery?.toString() || '');

        // Update joined tables tracking at runtime
        const joinName = options.as || table;
        (this.joinedTables as any)[joinName] = {
            table: {} as any,
            options: options
        };

        // Return the same instance but TypeScript will see it as the new joined type
        return this as any;
    }

    // Improved filter methods with better column type support
    eq<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: K extends AllAvailableColumns<TTable, TJoinedTables>
            ? ColumnValueOrReference<TTable, K extends TableColumns<TTable> ? K : never, TSchema, TParentTable, TJoinedTables>
            : any
    ): this {
        return this.addCondition({ column, operator: 'eq', value, isColumnReference: this.isColumnReference(value) });
    }

    neq<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: K extends AllAvailableColumns<TTable, TJoinedTables>
            ? ColumnValueOrReference<TTable, K extends TableColumns<TTable> ? K : never, TSchema, TParentTable, TJoinedTables>
            : any
    ): this {
        return this.addCondition({ column, operator: 'neq', value, isColumnReference: this.isColumnReference(value) });
    }

    gt<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: K extends AllAvailableColumns<TTable, TJoinedTables>
            ? ColumnValueOrReference<TTable, K extends TableColumns<TTable> ? K : never, TSchema, TParentTable, TJoinedTables>
            : any
    ): this {
        return this.addCondition({ column, operator: 'gt', value, isColumnReference: this.isColumnReference(value) });
    }

    gte<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: K extends AllAvailableColumns<TTable, TJoinedTables>
            ? ColumnValueOrReference<TTable, K extends TableColumns<TTable> ? K : never, TSchema, TParentTable, TJoinedTables>
            : any
    ): this {
        return this.addCondition({ column, operator: 'gte', value, isColumnReference: this.isColumnReference(value) });
    }

    lt<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: K extends AllAvailableColumns<TTable, TJoinedTables>
            ? ColumnValueOrReference<TTable, K extends TableColumns<TTable> ? K : never, TSchema, TParentTable, TJoinedTables>
            : any
    ): this {
        return this.addCondition({ column, operator: 'lt', value, isColumnReference: this.isColumnReference(value) });
    }

    lte<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: K extends AllAvailableColumns<TTable, TJoinedTables>
            ? ColumnValueOrReference<TTable, K extends TableColumns<TTable> ? K : never, TSchema, TParentTable, TJoinedTables>
            : any
    ): this {
        return this.addCondition({ column, operator: 'lte', value, isColumnReference: this.isColumnReference(value) });
    }

    like<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: string | `"${TableColumns<TTable>}"` | `"${TableColumns<TParentTable>}"` | `"${string}.${string}"`
    ): this {
        return this.addCondition({ column, operator: 'like', value, isColumnReference: this.isColumnReference(value) });
    }

    ilike<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: string | `"${TableColumns<TTable>}"` | `"${TableColumns<TParentTable>}"` | `"${string}.${string}"`
    ): this {
        return this.addCondition({ column, operator: 'ilike', value, isColumnReference: this.isColumnReference(value) });
    }

    contains<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: string | `"${TableColumns<TTable>}"` | `"${TableColumns<TParentTable>}"` | `"${string}.${string}"`
    ): this {
        return this.addCondition({ column, operator: 'contains', value, isColumnReference: this.isColumnReference(value) });
    }

    startswith<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: string | `"${TableColumns<TTable>}"` | `"${TableColumns<TParentTable>}"` | `"${string}.${string}"`
    ): this {
        return this.addCondition({ column, operator: 'startswith', value, isColumnReference: this.isColumnReference(value) });
    }

    endswith<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(
        column: K,
        value: string | `"${TableColumns<TTable>}"` | `"${TableColumns<TParentTable>}"` | `"${string}.${string}"`
    ): this {
        return this.addCondition({ column, operator: 'endswith', value, isColumnReference: this.isColumnReference(value) });
    }

    in<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(column: K, values: any[]): this {
        return this.addCondition({ column, operator: 'in', value: values });
    }

    nin<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(column: K, values: any[]): this {
        return this.addCondition({ column, operator: 'nin', value: values });
    }

    between<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(column: K, min: any, max: any): this {
        return this.addCondition({ column, operator: 'between', value: [min, max] });
    }

    nbetween<K extends AllAvailableColumns<TTable, TJoinedTables> | JsonPath<TTable, TJoinedTables>>(column: K, min: any, max: any): this {
        return this.addCondition({ column, operator: 'nbetween', value: [min, max] });
    }

    is<K extends AllAvailableColumns<TTable, TJoinedTables>>(column: K, value: null | boolean | 'null' | 'not_null'): this {
        return this.addCondition({ column, operator: 'is', value });
    }

    isnot<K extends AllAvailableColumns<TTable, TJoinedTables>>(column: K, value: null | boolean | 'null' | 'not_null'): this {
        return this.addCondition({ column, operator: 'isnot', value });
    }

    isNull<K extends AllAvailableColumns<TTable, TJoinedTables>>(column: K): this {
        return this.is(column, 'null');
    }

    isNotNull<K extends AllAvailableColumns<TTable, TJoinedTables>>(column: K): this {
        return this.is(column, 'not_null');
    }

    // ============ LOGICAL OPERATORS ============

    and(callback: (filter: TableQueryBuilder<TClient, TTable, TSchema, TResult, TParentTable, TJoinedTables>) => any): this {
        const nestedFilter = new TableQueryBuilder<TClient, TTable, TSchema, TResult, TParentTable, TJoinedTables>(this.client, this.config);
        // Copy joined tables context to nested filter
        nestedFilter.joinedTables = this.joinedTables;
        callback(nestedFilter);

        if (nestedFilter.conditions.length > 0) {
            this.conditions.push({
                operator: 'and',
                conditions: nestedFilter.getConditions()
            });
        }
        return this;
    }

    or(callback: (filter: TableQueryBuilder<TClient, TTable, TSchema, TResult, TParentTable, TJoinedTables>) => any): this {
        const nestedFilter = new TableQueryBuilder<TClient, TTable, TSchema, TResult, TParentTable, TJoinedTables>(this.client, this.config);
        // Copy joined tables context to nested filter
        nestedFilter.joinedTables = this.joinedTables;
        callback(nestedFilter);

        if (nestedFilter.conditions.length > 0) {
            this.conditions.push({
                operator: 'or',
                conditions: nestedFilter.getConditions()
            });
        }
        return this;
    }

    not(callback: (filter: TableQueryBuilder<TClient, TTable, TSchema, TResult, TParentTable, TJoinedTables>) => any): this {
        const nestedFilter = new TableQueryBuilder<TClient, TTable, TSchema, TResult, TParentTable, TJoinedTables>(this.client, this.config);
        // Copy joined tables context to nested filter
        nestedFilter.joinedTables = this.joinedTables;
        callback(nestedFilter);

        if (nestedFilter.conditions.length > 0) {
            this.conditions.push({
                operator: 'not',
                conditions: nestedFilter.getConditions()
            });
        }
        return this;
    }

    // ============ UTILITY METHODS ============

    protected isColumnReference(value: any): boolean {
        return typeof value === 'string' && value.startsWith('"') && value.endsWith('"');
    }

    protected addCondition(condition: NuvqlFilterCondition): this {
        this.conditions.push(condition);
        return this;
    }

    getConditions(): NuvqlCondition[] {
        return [...this.conditions];
    }

    getJoinedTables(): TJoinedTables {
        return this.joinedTables;
    }

    // Helper method to access joined table columns with type safety
    getJoinedTableColumns<TJoinName extends keyof TJoinedTables>(
        joinName: TJoinName
    ): TJoinedTables[TJoinName] extends DatabaseTypes.GenericTable
        ? TableColumns<TJoinedTables[TJoinName]>[]
        : never {
        // This is a runtime helper - actual column names would come from schema
        return [] as any;
    }

    // ============ QUERY EXECUTION ============

    async execute(): Promise<TResult[]> {
        // TODO: Implement actual query execution
        throw new NuvixException('Query execution not implemented', 500, 'NOT_IMPLEMENTED');
    }

    async single(): Promise<TResult> {
        const results = await this.execute();
        if (results.length === 0) {
            throw new NuvixException('No results found', 404, 'NO_RESULTS');
        }
        if (results.length > 1) {
            throw new NuvixException('Multiple results found', 400, 'MULTIPLE_RESULTS');
        }
        return results[0];
    }

    async maybeSingle(): Promise<TResult | null> {
        const results = await this.execute();
        return results.length > 0 ? results[0] : null;
    }

    // ============ QUERY STRING BUILDING ============

    toString(): string {
        const select = this.selectedColumns.length === 0 ? '*' : this.selectedColumns.join(',');
        const filter = this.conditions.map(c => this.buildCondition(c)).join(',');

        let query = `select=${select}`;
        if (filter) {
            query += `&filter=${filter}`;
        }

        // Handle joins
        if (this.joins.length > 0) {
            query += `,${this.joins.join(',')}`;
        }

        // Handle join options for join builders
        if (this.config.joinOptions) {
            const opts = this.config.joinOptions;
            const type = opts.type ?? 'left';
            const flatten = 'flatten' in opts && opts.flatten ? '...' : '';
            const shape = !flatten && 'shape' in opts ? `.${opts.shape ?? 'many'}` : '';
            const alias = opts.as ? `${opts.as}:` : '';

            return `${flatten}${alias}${this.config.tableName}${shape}{${filter},$.join(${type})}(${select})`;
        }

        return query;
    }

    private buildCondition(condition: NuvqlCondition): string {
        if ('operator' in condition && ['and', 'or', 'not'].includes(condition.operator)) {
            const logical = condition as NuvqlLogicalCondition;
            const built = logical.conditions.map(c => this.buildCondition(c));

            switch (logical.operator) {
                case 'and': return `and(${built.join(',')})`;
                case 'or': return `or(${built.join(',')})`;
                case 'not': return `not(${built.join(',')})`;
                default: return built.join(',');
            }
        } else {
            const filter = condition as NuvqlFilterCondition;
            const { column, operator, value, isColumnReference } = filter;

            switch (operator) {
                case 'eq': return `${column}.eq(${this.formatValue(value, isColumnReference)})`;
                case 'neq': return `${column}.neq(${this.formatValue(value, isColumnReference)})`;
                case 'gt': return `${column}.gt(${this.formatValue(value, isColumnReference)})`;
                case 'gte': return `${column}.gte(${this.formatValue(value, isColumnReference)})`;
                case 'lt': return `${column}.lt(${this.formatValue(value, isColumnReference)})`;
                case 'lte': return `${column}.lte(${this.formatValue(value, isColumnReference)})`;
                case 'like': return `${column}.like(${this.formatValue(value, isColumnReference)})`;
                case 'ilike': return `${column}.ilike(${this.formatValue(value, isColumnReference)})`;
                case 'contains': return `${column}.contains(${this.formatValue(value, isColumnReference)})`;
                case 'startswith': return `${column}.startswith(${this.formatValue(value, isColumnReference)})`;
                case 'endswith': return `${column}.endswith(${this.formatValue(value, isColumnReference)})`;
                case 'in':
                    const inValues = Array.isArray(value) ? value.map(v => this.formatValue(v)).join(',') : this.formatValue(value, isColumnReference);
                    return `${column}.in[${inValues}]`;
                case 'nin':
                    const ninValues = Array.isArray(value) ? value.map(v => this.formatValue(v)).join(',') : this.formatValue(value, isColumnReference);
                    return `${column}.nin[${ninValues}]`;
                case 'between':
                    const [min, max] = Array.isArray(value) ? value : [value, value];
                    return `${column}.between[${this.formatValue(min)},${this.formatValue(max)}]`;
                case 'nbetween':
                    const [nMin, nMax] = Array.isArray(value) ? value : [value, value];
                    return `${column}.nbetween[${this.formatValue(nMin)},${this.formatValue(nMax)}]`;
                case 'is': return `${column}.is(${this.formatSpecialValue(value)})`;
                case 'isnot': return `${column}.isnot(${this.formatSpecialValue(value)})`;
                default: return `${column}.eq(${this.formatValue(value, isColumnReference)})`;
            }
        }
    }

    private formatValue(value: any, isColumnReference?: boolean): string {
        // Handle column references
        if (isColumnReference && typeof value === 'string' && value.startsWith('"') && value.endsWith('"')) {
            return value.slice(1, -1); // Remove quotes for column references
        }

        if (value === null || value === undefined) return 'null';
        if (typeof value === 'string') return `'${value.replace(/'/g, "\\'")}'`;
        if (typeof value === 'boolean') return value ? 'true' : 'false';
        if (typeof value === 'number') return String(value);
        if (value instanceof Date) return `'${value.toISOString()}'`;
        return `'${JSON.stringify(value).replace(/'/g, "\\'")}'`;
    }

    private formatSpecialValue(value: any): string {
        if (value === null || value === 'null') return 'null';
        if (value === 'not_null') return 'not_null';
        if (typeof value === 'boolean') return value ? 'true' : 'false';
        return this.formatValue(value);
    }
}
