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

// Column builder for fluent API
export class ColumnBuilder {
    constructor(private columnName: string) { }

    // Comparison operators
    eq(value: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'eq',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    neq(value: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'neq',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    gt(value: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'gt',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    gte(value: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'gte',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    lt(value: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'lt',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    lte(value: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'lte',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    // Array operators
    in(values: any[]): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'in',
            value: values
        };
    }

    nin(values: any[]): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'nin',
            value: values
        };
    }

    // String operators
    like(value: string): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'like',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    ilike(value: string): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'ilike',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    contains(value: string): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'contains',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    startswith(value: string): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'startswith',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    endswith(value: string): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'endswith',
            value,
            isColumnReference: this.isColumnReference(value)
        };
    }

    // Null checks
    is(value: null | boolean | 'null' | 'not_null'): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'is',
            value
        };
    }

    isnot(value: null | boolean | 'null' | 'not_null'): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'isnot',
            value
        };
    }

    // Range operators
    between(min: any, max: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'between',
            value: [min, max]
        };
    }

    nbetween(min: any, max: any): NuvqlFilterCondition {
        return {
            column: this.columnName,
            operator: 'nbetween',
            value: [min, max]
        };
    }

    // Helper methods
    isNull(): NuvqlFilterCondition {
        return this.is('null');
    }

    isNotNull(): NuvqlFilterCondition {
        return this.is('not_null');
    }

    isEmpty(): NuvqlFilterCondition {
        return this.eq('');
    }

    isNotEmpty(): NuvqlFilterCondition {
        return this.neq('');
    }

    private isColumnReference(value: any): boolean {
        return typeof value === 'string' && value.startsWith('"') && value.endsWith('"');
    }
}

// Logical operators
export function and(...conditions: NuvqlCondition[]): NuvqlLogicalCondition {
    return {
        operator: 'and',
        conditions
    };
}

export function or(...conditions: NuvqlCondition[]): NuvqlLogicalCondition {
    return {
        operator: 'or',
        conditions
    };
}

export function not(...conditions: NuvqlCondition[]): NuvqlLogicalCondition {
    return {
        operator: 'not',
        conditions
    };
}

// Column factory function
export function column(name: string): ColumnBuilder {
    return new ColumnBuilder(name);
}

// Filter class for building NUVQL filters
export class NuvqlFilter {
    private conditions: NuvqlCondition[] = [];

    // Add a condition
    add(condition: NuvqlCondition): this {
        this.conditions.push(condition);
        return this;
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
            return this.conditionToString(this.conditions[0]);
        }

        // Multiple conditions are implicitly AND-ed
        return this.conditionToString({
            operator: 'and',
            conditions: this.conditions
        });
    }

    private conditionToString(condition: NuvqlCondition): string {
        if ('operator' in condition && ['and', 'or', 'not'].includes(condition.operator)) {
            const logicalCondition = condition as NuvqlLogicalCondition;
            return this.logicalConditionToString(logicalCondition);
        } else {
            const filterCondition = condition as NuvqlFilterCondition;
            return this.filterConditionToString(filterCondition);
        }
    }

    private logicalConditionToString(condition: NuvqlLogicalCondition): string {
        const conditionsStr = condition.conditions
            .map(c => this.conditionToString(c))
            .join(',');

        switch (condition.operator) {
            case 'and':
                return `and(${conditionsStr})`;
            case 'or':
                return `or(${conditionsStr})`;
            case 'not':
                return `not(${conditionsStr})`;
            default:
                return conditionsStr;
        }
    }

    private filterConditionToString(condition: NuvqlFilterCondition): string {
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
    static from(...conditions: NuvqlCondition[]): NuvqlFilter {
        const filter = new NuvqlFilter();
        conditions.forEach(condition => filter.add(condition));
        return filter;
    }

    // Clear all conditions
    clear(): this {
        this.conditions = [];
        return this;
    }

    // Check if filter is empty
    isEmpty(): boolean {
        return this.conditions.length === 0;
    }
}

// Type-safe column builder for specific table types
export class TypedColumnBuilder<Table> {
    constructor(private columnName: keyof Table) { }

    // Comparison operators with type safety
    eq(value: Table[keyof Table]): NuvqlFilterCondition {
        return column(String(this.columnName)).eq(value);
    }

    neq(value: Table[keyof Table]): NuvqlFilterCondition {
        return column(String(this.columnName)).neq(value);
    }

    gt(value: Table[keyof Table]): NuvqlFilterCondition {
        return column(String(this.columnName)).gt(value);
    }

    gte(value: Table[keyof Table]): NuvqlFilterCondition {
        return column(String(this.columnName)).gte(value);
    }

    lt(value: Table[keyof Table]): NuvqlFilterCondition {
        return column(String(this.columnName)).lt(value);
    }

    lte(value: Table[keyof Table]): NuvqlFilterCondition {
        return column(String(this.columnName)).lte(value);
    }

    in(values: Table[keyof Table][]): NuvqlFilterCondition {
        return column(String(this.columnName)).in(values);
    }

    nin(values: Table[keyof Table][]): NuvqlFilterCondition {
        return column(String(this.columnName)).nin(values);
    }

    between(min: Table[keyof Table], max: Table[keyof Table]): NuvqlFilterCondition {
        return column(String(this.columnName)).between(min, max);
    }

    // String-specific methods
    like(value: string): NuvqlFilterCondition {
        return column(String(this.columnName)).like(value);
    }

    ilike(value: string): NuvqlFilterCondition {
        return column(String(this.columnName)).ilike(value);
    }

    contains(value: string): NuvqlFilterCondition {
        return column(String(this.columnName)).contains(value);
    }

    startswith(value: string): NuvqlFilterCondition {
        return column(String(this.columnName)).startswith(value);
    }

    endswith(value: string): NuvqlFilterCondition {
        return column(String(this.columnName)).endswith(value);
    }

    // Null checks
    isNull(): NuvqlFilterCondition {
        return column(String(this.columnName)).isNull();
    }

    isNotNull(): NuvqlFilterCondition {
        return column(String(this.columnName)).isNotNull();
    }

    // Special value checks
    is(value: null | boolean | 'null' | 'not_null'): NuvqlFilterCondition {
        return column(String(this.columnName)).is(value);
    }

    isnot(value: null | boolean | 'null' | 'not_null'): NuvqlFilterCondition {
        return column(String(this.columnName)).isnot(value);
    }
}

// Type-safe column factory
export function typedColumn<Table>(name: keyof Table): TypedColumnBuilder<Table> {
    return new TypedColumnBuilder(name);
}
