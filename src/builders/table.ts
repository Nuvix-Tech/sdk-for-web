import type { Client } from "../client";
import { DatabaseTypes } from "./types";
import { Cast, Column, column } from "./utils";

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

type LogicalFilter<TableQueryBuilder> = Omit<TableQueryBuilder, 'select'>;
type Filter<TableQueryBuilder, Sub extends boolean = false> = Sub extends true ? Omit<TableQueryBuilder, 'select'> : TableQueryBuilder;

type RawColumn<C extends string, A extends unknown> = A extends string ? `${A}:${C}` | `${A}:${C}::${Cast}` : `${C}::${Cast}`

export class TableQueryBuilder<T extends Client, Table extends DatabaseTypes.GenericTable, SchemasTypes = unknown, SelectTyeps = Table['Row'], Sub extends boolean = false> {
    private client: T;
    private tableName: string;
    private schema: string;
    private selectedColumns: string[] = [];
    private conditions: NuvqlCondition[] = [];

    constructor(client: T, { tableName, schema }: { tableName: string, schema: string }) {
        this.client = client;
        this.tableName = tableName;
        this.schema = schema;
    }

    // SELECT operations with better type safety
    select(): TableQueryBuilder<T, Table, SchemasTypes>;
    select(columns: '*'): TableQueryBuilder<T, Table, SchemasTypes>;
    select<K extends string & keyof Table['Row'], A extends string, _A extends unknown, __A extends unknown>(...columns: (Record<A, K> | K | RawColumn<K, _A> | Column<K, __A>)[]): TableQueryBuilder<T, Table, SchemasTypes, { Row: Omit<Table['Row'], K> & Record<A, Table['Row'][K]> & Record<_A extends string ? _A : K, Table['Row'][K]> & Record<__A extends string ? __A : K, Table['Row'][K]> }>;
    select<K extends keyof Table['Row']>(...columns: K[]): TableQueryBuilder<T, any, SchemasTypes> {
        this.selectedColumns = columns as string[];
        return this as any;
    }

    eq<C extends keyof Table['Row']>(column: C, value: Table['Row'][C]): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'eq',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    neq(column: string, value: any): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'neq',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    gt(column: string, value: any): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'gt',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    gte(column: string, value: any): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'gte',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    lt(column: string, value: any): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'lt',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    lte(column: string, value: any): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'lte',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    // Text operators
    like(column: string, value: string): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'like',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    ilike(column: string, value: string): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'ilike',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    contains(column: string, value: string): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'contains',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    startswith(column: string, value: string): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'startswith',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    endswith(column: string, value: string): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'endswith',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    // Array operators
    in(column: string, values: any[]): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'in',
            value: values
        });
    }

    nin(column: string, values: any[]): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'nin',
            value: values
        });
    }

    // Range operators
    between(column: string, min: any, max: any): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'between',
            value: [min, max]
        });
    }

    nbetween(column: string, min: any, max: any): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'nbetween',
            value: [min, max]
        });
    }

    // Null/existence operators
    is(column: string, value: null | boolean | 'null' | 'not_null'): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'is',
            value
        });
    }

    isnot(column: string, value: null | boolean | 'null' | 'not_null'): Filter<typeof this, Sub> {
        return this.addCondition({
            column,
            operator: 'isnot',
            value
        });
    }

    // Convenience methods
    isNull(column: string) {
        return this.is(column, 'null');
    }

    isNotNull(column: string) {
        return this.is(column, 'not_null');
    }

    isEmptyString(column: string) {
        return this.eq(column, '' as any);
    }

    isNotEmptyString(column: string) {
        return this.neq(column, '');
    }

    // Logical operators - Knex-like chaining
    and(callback: (filter: LogicalFilter<TableQueryBuilder<T, Table, SchemasTypes, true>>) => LogicalFilter<TableQueryBuilder<T, Table, SchemasTypes, true>>): Filter<typeof this, Sub> {
        const nestedFilter = callback(new TableQueryBuilder(this.client, { tableName: this.tableName, schema: this.schema }));
        const nestedConditions = nestedFilter.getConditions();

        if (nestedConditions.length > 0) {
            const andCondition: NuvqlLogicalCondition = {
                operator: 'and',
                conditions: nestedConditions
            };
            this.conditions.push(andCondition);
        }
        return this;
    }

    or(callback: (filter: LogicalFilter<TableQueryBuilder<T, Table, SchemasTypes, true>>) => LogicalFilter<TableQueryBuilder<T, Table, SchemasTypes, true>>): Filter<typeof this, Sub> {
        const nestedFilter = callback(new TableQueryBuilder(this.client, { tableName: this.tableName, schema: this.schema }));
        const nestedConditions = nestedFilter.getConditions();

        if (nestedConditions.length > 0) {
            const orCondition: NuvqlLogicalCondition = {
                operator: 'or',
                conditions: nestedConditions
            };
            this.conditions.push(orCondition)
        }
        return this;
    }

    not(callback: (filter: LogicalFilter<TableQueryBuilder<T, Table, SchemasTypes, true>>) => LogicalFilter<TableQueryBuilder<T, Table, SchemasTypes, true>>): Filter<typeof this, Sub> {
        const nestedFilter = callback(new TableQueryBuilder(this.client, { tableName: this.tableName, schema: this.schema }));
        const nestedConditions = nestedFilter.getConditions();

        if (nestedConditions.length > 0) {
            const notCondition: NuvqlLogicalCondition = {
                operator: 'not',
                conditions: nestedConditions
            };
            this.conditions.push(notCondition);
        }
        return this;
    }

    // Helper methods
    private addCondition(condition: NuvqlFilterCondition) {
        this.conditions.push(condition);
        return this;
    }

    private isColumnReference(value: any): boolean {
        return typeof value === 'string' && value.startsWith('"') && value.endsWith('"');
    }

    // Get all conditions
    getConditions(): NuvqlCondition[] {
        return [...this.conditions];
    }

    // Convert to NUVQL filter string
    toString(): string {
        if (this.conditions.length === 0) {
            return '';
        }

        if (this.conditions.length === 1) {
            return this.buildCondition(this.conditions[0]);
        }

        // Multiple conditions are implicitly ANDed
        return this.conditions.map(condition => this.buildCondition(condition)).join(',');
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
    Row: { name: string, sduud: "gello", id: number };
    Insert: { name: string, sduud: "gello", id: number };
    Update: { name: string, sduud: "gello", id: number };
    Delete: { name: string, sduud: "gello", id: number }
    Relationships: []
}
// Example usage:
const queryBuilder = new TableQueryBuilder<any, Users, any>({} as Client, { tableName: 'users', schema: 'public' });

const res = queryBuilder
    .select(
        'uu:id', 'name',
        { io: "name", uu: "sduud", kk: "id" },
        column('name').cast('text'),
        column('id').as('$id'),
    )
    // s => s('').select().filter(),
    .or(topOr => topOr
        .eq('sduud', 'gello')
        .or(nestOr =>
            nestOr.gt('age', 18)
                .ilike('name', '%john%')
        )
        .not(notFilter =>
            notFilter.in('role', ['admin', 'user'])
        )
        .gt('age', 18)
        .ilike('name', '%john%')
        .in('role', ['admin', 'user'])
        .between('created_at', '2023-01-01', '2023-12-31')
    ).toString()

console.log(res); // For debugging, you can implement a toString method to see the query structure
