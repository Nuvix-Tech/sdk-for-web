/**
 * NUVQL Filter Builder - Knex-like chainable API
 * Supports: eq(column, value), and(), or(), not() with full chaining
 */

// NUVQL Filter Types and Operators
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

// Main NUVQL Filter Builder - Knex-like chainable API
export class NuvqlFilter {
    private conditions: NuvqlCondition[] = [];

    constructor(conditions: NuvqlCondition[] = []) {
        this.conditions = [...conditions];
    }

    // Comparison operators - Knex-like: eq(column, value)
    eq(column: string, value: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'eq',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    neq(column: string, value: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'neq',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    gt(column: string, value: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'gt',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    gte(column: string, value: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'gte',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    lt(column: string, value: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'lt',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    lte(column: string, value: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'lte',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    // Text operators
    like(column: string, value: string): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'like',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    ilike(column: string, value: string): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'ilike',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    contains(column: string, value: string): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'contains',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    startswith(column: string, value: string): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'startswith',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    endswith(column: string, value: string): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'endswith',
            value,
            isColumnReference: this.isColumnReference(value)
        });
    }

    // Array operators
    in(column: string, values: any[]): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'in',
            value: values
        });
    }

    nin(column: string, values: any[]): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'nin',
            value: values
        });
    }

    // Range operators
    between(column: string, min: any, max: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'between',
            value: [min, max]
        });
    }

    nbetween(column: string, min: any, max: any): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'nbetween',
            value: [min, max]
        });
    }

    // Null/existence operators
    is(column: string, value: null | boolean | 'null' | 'not_null'): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'is',
            value
        });
    }

    isnot(column: string, value: null | boolean | 'null' | 'not_null'): NuvqlFilter {
        return this.addCondition({
            column,
            operator: 'isnot',
            value
        });
    }

    // Convenience methods
    isNull(column: string): NuvqlFilter {
        return this.is(column, 'null');
    }

    isNotNull(column: string): NuvqlFilter {
        return this.is(column, 'not_null');
    }

    isEmptyString(column: string): NuvqlFilter {
        return this.eq(column, '');
    }

    isNotEmptyString(column: string): NuvqlFilter {
        return this.neq(column, '');
    }

    // Logical operators - Knex-like chaining
    and(callback: (filter: NuvqlFilter) => NuvqlFilter): NuvqlFilter {
        const nestedFilter = callback(new NuvqlFilter());
        const nestedConditions = nestedFilter.getConditions();

        if (nestedConditions.length > 0) {
            const andCondition: NuvqlLogicalCondition = {
                operator: 'and',
                conditions: nestedConditions
            };
            return new NuvqlFilter([...this.conditions, andCondition]);
        }
        return this;
    }

    or(callback: (filter: NuvqlFilter) => NuvqlFilter): NuvqlFilter {
        const nestedFilter = callback(new NuvqlFilter());
        const nestedConditions = nestedFilter.getConditions();

        if (nestedConditions.length > 0) {
            const orCondition: NuvqlLogicalCondition = {
                operator: 'or',
                conditions: nestedConditions
            };
            return new NuvqlFilter([...this.conditions, orCondition]);
        }
        return this;
    }

    not(callback: (filter: NuvqlFilter) => NuvqlFilter): NuvqlFilter {
        const nestedFilter = callback(new NuvqlFilter());
        const nestedConditions = nestedFilter.getConditions();

        if (nestedConditions.length > 0) {
            const notCondition: NuvqlLogicalCondition = {
                operator: 'not',
                conditions: nestedConditions
            };
            return new NuvqlFilter([...this.conditions, notCondition]);
        }
        return this;
    }

    // Helper methods
    private addCondition(condition: NuvqlFilterCondition): NuvqlFilter {
        return new NuvqlFilter([...this.conditions, condition]);
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

    // Static methods for convenience
    static create(): NuvqlFilter {
        return new NuvqlFilter();
    }

    // Clear all conditions
    clear(): NuvqlFilter {
        return new NuvqlFilter();
    }

    // Check if filter is empty
    isEmpty(): boolean {
        return this.conditions.length === 0;
    }

    // Clone the filter
    clone(): NuvqlFilter {
        return new NuvqlFilter([...this.conditions]);
    }
}

// Type-safe filter builder
export class TypedNuvqlFilter<T extends Record<string, any>> {
    private filter: NuvqlFilter;

    constructor(filter?: NuvqlFilter) {
        this.filter = filter || new NuvqlFilter();
    }

    // Type-safe comparison operators
    eq<K extends keyof T>(column: K, value: T[K] | string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.eq(String(column), value));
    }

    neq<K extends keyof T>(column: K, value: T[K] | string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.neq(String(column), value));
    }

    gt<K extends keyof T>(column: K, value: T[K] | string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.gt(String(column), value));
    }

    gte<K extends keyof T>(column: K, value: T[K] | string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.gte(String(column), value));
    }

    lt<K extends keyof T>(column: K, value: T[K] | string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.lt(String(column), value));
    }

    lte<K extends keyof T>(column: K, value: T[K] | string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.lte(String(column), value));
    }

    // Type-safe text operators
    like<K extends keyof T>(column: K, value: string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.like(String(column), value));
    }

    ilike<K extends keyof T>(column: K, value: string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.ilike(String(column), value));
    }

    contains<K extends keyof T>(column: K, value: string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.contains(String(column), value));
    }

    startswith<K extends keyof T>(column: K, value: string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.startswith(String(column), value));
    }

    endswith<K extends keyof T>(column: K, value: string): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.endswith(String(column), value));
    }

    // Type-safe array operators
    in<K extends keyof T>(column: K, values: T[K][]): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.in(String(column), values));
    }

    nin<K extends keyof T>(column: K, values: T[K][]): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.nin(String(column), values));
    }

    // Type-safe range operators
    between<K extends keyof T>(column: K, min: T[K], max: T[K]): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.between(String(column), min, max));
    }

    nbetween<K extends keyof T>(column: K, min: T[K], max: T[K]): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.nbetween(String(column), min, max));
    }

    // Type-safe null operators
    is<K extends keyof T>(column: K, value: null | boolean | 'null' | 'not_null'): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.is(String(column), value));
    }

    isnot<K extends keyof T>(column: K, value: null | boolean | 'null' | 'not_null'): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.isnot(String(column), value));
    }

    // Type-safe convenience methods
    isNull<K extends keyof T>(column: K): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.isNull(String(column)));
    }

    isNotNull<K extends keyof T>(column: K): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.isNotNull(String(column)));
    }

    isEmptyString<K extends keyof T>(column: K): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.isEmptyString(String(column)));
    }

    isNotEmptyString<K extends keyof T>(column: K): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.isNotEmptyString(String(column)));
    }

    // Type-safe logical operators
    and(callback: (filter: TypedNuvqlFilter<T>) => TypedNuvqlFilter<T>): TypedNuvqlFilter<T> {
        const typedFilter = this.filter.and((f) => {
            const typedF = new TypedNuvqlFilter<T>(f);
            return callback(typedF).build();
        });
        return new TypedNuvqlFilter<T>(typedFilter);
    }

    or(callback: (filter: TypedNuvqlFilter<T>) => TypedNuvqlFilter<T>): TypedNuvqlFilter<T> {
        const typedFilter = this.filter.or((f) => {
            const typedF = new TypedNuvqlFilter<T>(f);
            return callback(typedF).build();
        });
        return new TypedNuvqlFilter<T>(typedFilter);
    }

    not(callback: (filter: TypedNuvqlFilter<T>) => TypedNuvqlFilter<T>): TypedNuvqlFilter<T> {
        const typedFilter = this.filter.not((f) => {
            const typedF = new TypedNuvqlFilter<T>(f);
            return callback(typedF).build();
        });
        return new TypedNuvqlFilter<T>(typedFilter);
    }

    // Build methods
    build(): NuvqlFilter {
        return this.filter;
    }

    toString(): string {
        return this.filter.toString();
    }

    // Utility methods
    clear(): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.clear());
    }

    isEmpty(): boolean {
        return this.filter.isEmpty();
    }

    clone(): TypedNuvqlFilter<T> {
        return new TypedNuvqlFilter<T>(this.filter.clone());
    }
}

// Factory functions
export function filter(): NuvqlFilter {
    return new NuvqlFilter();
}

export function typedFilter<T extends Record<string, any>>(): TypedNuvqlFilter<T> {
    return new TypedNuvqlFilter<T>();
}

// Export aliases for convenience
export { NuvqlFilter as Filter };
export { TypedNuvqlFilter as TypedFilter };
