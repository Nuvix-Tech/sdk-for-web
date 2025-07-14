/**
 * NUVQL Select Query Builder
 * Supports columns, aliases, types, and embedded resources
 * Syntax: column, alias:column, alias:column:type, alias:table.one|many{filter}(select)
 */

import { NuvqlFilter } from './filter';

// Base types for select operations
export type SelectColumn = string;
export type SelectAlias = string;
export type SelectType = 'string' | 'number' | 'boolean' | 'date' | 'json' | 'array';
export type EmbedType = 'one' | 'many';

// Column selection types
export interface ColumnSpec {
    column: string;
    alias?: string;
    type?: SelectType;
}

// Embedded resource types
export interface EmbedSpec {
    alias: string;
    table: string;
    type: EmbedType;
    filter?: NuvqlFilter;
    select?: NuvqlSelect;
}

// Union type for all select specifications
export type SelectSpec = ColumnSpec | EmbedSpec;

// Type guards
export function isColumnSpec(spec: SelectSpec): spec is ColumnSpec {
    return 'column' in spec && !('table' in spec);
}

export function isEmbedSpec(spec: SelectSpec): spec is EmbedSpec {
    return 'table' in spec && 'type' in spec;
}

// Main Select Query Builder Class
export class NuvqlSelect {
    private specs: SelectSpec[] = [];

    constructor(specs: SelectSpec[] = []) {
        this.specs = [...specs];
    }

    // Column selection with various overloads
    column(column: string): NuvqlSelect;
    column(column: string, alias: string): NuvqlSelect;
    column(column: string, alias: string, type: SelectType): NuvqlSelect;
    column(column: string, alias?: string, type?: SelectType): NuvqlSelect {
        const spec: ColumnSpec = { column };
        if (alias) spec.alias = alias;
        if (type) spec.type = type;

        return new NuvqlSelect([...this.specs, spec]);
    }

    // Multiple columns at once
    columns(...columns: string[]): NuvqlSelect {
        const newSpecs = columns.map(col => ({ column: col }) as ColumnSpec);
        return new NuvqlSelect([...this.specs, ...newSpecs]);
    }

    // Aliased column selection
    as(column: string, alias: string, type?: SelectType): NuvqlSelect {
        const spec: ColumnSpec = { column, alias };
        if (type) spec.type = type;

        return new NuvqlSelect([...this.specs, spec]);
    }

    // Typed column selection
    typed(column: string, type: SelectType, alias?: string): NuvqlSelect {
        const spec: ColumnSpec = { column, type };
        if (alias) spec.alias = alias;

        return new NuvqlSelect([...this.specs, spec]);
    }

    // Embedded resource selection - one-to-one relationship
    embedOne(alias: string, table: string): EmbedBuilder {
        return new EmbedBuilder(this, alias, table, 'one');
    }

    // Embedded resource selection - one-to-many relationship
    embedMany(alias: string, table: string): EmbedBuilder {
        return new EmbedBuilder(this, alias, table, 'many');
    }

    // Generic embed method
    embed(alias: string, table: string, type: EmbedType): EmbedBuilder {
        return new EmbedBuilder(this, alias, table, type);
    }

    // Build the select query string
    build(): string {
        if (this.specs.length === 0) {
            return '*';
        }

        return this.specs.map(spec => {
            if (isColumnSpec(spec)) {
                return this.buildColumnSpec(spec);
            } else {
                return this.buildEmbedSpec(spec);
            }
        }).join(',');
    }

    private buildColumnSpec(spec: ColumnSpec): string {
        let result = spec.column;

        if (spec.alias || spec.type) {
            result = `${spec.alias || spec.column}:${spec.column}`;

            if (spec.type) {
                result += `:${spec.type}`;
            }
        }

        return result;
    }

    private buildEmbedSpec(spec: EmbedSpec): string {
        let result = `${spec.alias}:${spec.table}.${spec.type}`;

        // Add filter if present
        if (spec.filter) {
            result += `{${spec.filter.toString()}}`;
        }

        // Add select if present
        if (spec.select) {
            result += `(${spec.select.build()})`;
        }

        return result;
    }

    // Get all specs for debugging
    getSpecs(): SelectSpec[] {
        return [...this.specs];
    }

    // Clone the current select builder
    clone(): NuvqlSelect {
        return new NuvqlSelect([...this.specs]);
    }

    // Check if select is empty
    isEmpty(): boolean {
        return this.specs.length === 0;
    }

    // Get column count
    getColumnCount(): number {
        return this.specs.filter(isColumnSpec).length;
    }

    // Get embed count
    getEmbedCount(): number {
        return this.specs.filter(isEmbedSpec).length;
    }

    // Static factory methods
    static create(): NuvqlSelect {
        return new NuvqlSelect();
    }

    static from(...columns: string[]): NuvqlSelect {
        return new NuvqlSelect().columns(...columns);
    }

    static column(column: string, alias?: string, type?: SelectType): NuvqlSelect {
        const select = new NuvqlSelect();
        if (alias && type) {
            return select.column(column, alias, type);
        } else if (alias) {
            return select.column(column, alias);
        } else {
            return select.column(column);
        }
    }
}

// Embed Builder for chaining embedded resource configuration
export class EmbedBuilder {
    constructor(
        private selectBuilder: NuvqlSelect,
        private alias: string,
        private table: string,
        private type: EmbedType,
        private filterBuilder?: NuvqlFilter,
        private selectChild?: NuvqlSelect
    ) { }

    // Add filter to the embedded resource
    filter(callback: (filter: NuvqlFilter) => NuvqlFilter): EmbedBuilder;
    filter(filter: NuvqlFilter): EmbedBuilder;
    filter(filterOrCallback: NuvqlFilter | ((filter: NuvqlFilter) => NuvqlFilter)): EmbedBuilder {
        let filter: NuvqlFilter;

        if (typeof filterOrCallback === 'function') {
            filter = filterOrCallback(new NuvqlFilter());
        } else {
            filter = filterOrCallback;
        }

        return new EmbedBuilder(
            this.selectBuilder,
            this.alias,
            this.table,
            this.type,
            filter,
            this.selectChild
        );
    }

    // Add select to the embedded resource
    select(callback: (select: NuvqlSelect) => NuvqlSelect): EmbedBuilder;
    select(select: NuvqlSelect): EmbedBuilder;
    select(...columns: string[]): EmbedBuilder;
    select(
        selectOrCallback: NuvqlSelect | ((select: NuvqlSelect) => NuvqlSelect) | string,
        ...columns: string[]
    ): EmbedBuilder {
        let select: NuvqlSelect;

        if (typeof selectOrCallback === 'function') {
            select = selectOrCallback(new NuvqlSelect());
        } else if (typeof selectOrCallback === 'string') {
            select = new NuvqlSelect().columns(selectOrCallback, ...columns);
        } else {
            select = selectOrCallback;
        }

        return new EmbedBuilder(
            this.selectBuilder,
            this.alias,
            this.table,
            this.type,
            this.filterBuilder,
            select
        );
    }

    // Complete the embed and return to the main select builder
    build(): NuvqlSelect {
        const embedSpec: EmbedSpec = {
            alias: this.alias,
            table: this.table,
            type: this.type,
            filter: this.filterBuilder,
            select: this.selectChild
        };

        return new NuvqlSelect([...this.selectBuilder.getSpecs(), embedSpec]);
    }

    // Shorthand methods for common operations
    where(callback: (filter: NuvqlFilter) => NuvqlFilter): EmbedBuilder {
        return this.filter(callback);
    }

    only(...columns: string[]): EmbedBuilder {
        return this.select(...columns);
    }
}

// Type-safe column builder for specific table types
export class TypedSelectBuilder<T extends Record<string, any>> {
    private selectBuilder: NuvqlSelect;

    constructor(selectBuilder?: NuvqlSelect) {
        this.selectBuilder = selectBuilder || new NuvqlSelect();
    }

    // Type-safe column selection
    column<K extends keyof T>(column: K): TypedSelectBuilder<T>;
    column<K extends keyof T>(column: K, alias: string): TypedSelectBuilder<T>;
    column<K extends keyof T>(column: K, alias: string, type: SelectType): TypedSelectBuilder<T>;
    column<K extends keyof T>(column: K, alias?: string, type?: SelectType): TypedSelectBuilder<T> {
        let newSelect: NuvqlSelect;
        if (alias && type) {
            newSelect = this.selectBuilder.column(String(column), alias, type);
        } else if (alias) {
            newSelect = this.selectBuilder.column(String(column), alias);
        } else {
            newSelect = this.selectBuilder.column(String(column));
        }
        return new TypedSelectBuilder<T>(newSelect);
    }

    // Type-safe multiple columns
    columns<K extends keyof T>(...columns: K[]): TypedSelectBuilder<T> {
        const newSelect = this.selectBuilder.columns(...columns.map(String));
        return new TypedSelectBuilder<T>(newSelect);
    }

    // Type-safe aliased column
    as<K extends keyof T>(column: K, alias: string, type?: SelectType): TypedSelectBuilder<T> {
        const newSelect = this.selectBuilder.as(String(column), alias, type);
        return new TypedSelectBuilder<T>(newSelect);
    }

    // Type-safe typed column
    typed<K extends keyof T>(column: K, type: SelectType, alias?: string): TypedSelectBuilder<T> {
        const newSelect = this.selectBuilder.typed(String(column), type, alias);
        return new TypedSelectBuilder<T>(newSelect);
    }

    // Embed with type safety for related tables
    embedOne<R extends Record<string, any>>(
        alias: string,
        table: string
    ): TypedEmbedBuilder<T, R> {
        return new TypedEmbedBuilder<T, R>(this.selectBuilder, alias, table, 'one');
    }

    embedMany<R extends Record<string, any>>(
        alias: string,
        table: string
    ): TypedEmbedBuilder<T, R> {
        return new TypedEmbedBuilder<T, R>(this.selectBuilder, alias, table, 'many');
    }

    // Build the final select
    build(): NuvqlSelect {
        return this.selectBuilder;
    }

    // Get the query string
    toString(): string {
        return this.selectBuilder.build();
    }
}

// Type-safe embed builder
export class TypedEmbedBuilder<T extends Record<string, any>, R extends Record<string, any>> {
    private embedBuilder: EmbedBuilder;

    constructor(
        selectBuilder: NuvqlSelect,
        alias: string,
        table: string,
        type: EmbedType
    ) {
        this.embedBuilder = new EmbedBuilder(selectBuilder, alias, table, type);
    }

    // Type-safe filter for the embedded resource
    filter(callback: (filter: NuvqlFilter) => NuvqlFilter): TypedEmbedBuilder<T, R> {
        this.embedBuilder = this.embedBuilder.filter(callback);
        return this;
    }

    // Type-safe select for the embedded resource
    select(callback: (select: TypedSelectBuilder<R>) => TypedSelectBuilder<R>): TypedEmbedBuilder<T, R> {
        const typedSelect = callback(new TypedSelectBuilder<R>());
        this.embedBuilder = this.embedBuilder.select(typedSelect.build());
        return this;
    }

    // Type-safe column selection for embedded resource
    columns<K extends keyof R>(...columns: K[]): TypedEmbedBuilder<T, R> {
        this.embedBuilder = this.embedBuilder.select(...columns.map(String));
        return this;
    }

    // Build and return to typed select builder
    build(): TypedSelectBuilder<T> {
        const newSelect = this.embedBuilder.build();
        return new TypedSelectBuilder<T>(newSelect);
    }
}

// Utility functions
export function select(): NuvqlSelect {
    return new NuvqlSelect();
}

export function selectColumns(...columns: string[]): NuvqlSelect {
    return new NuvqlSelect().columns(...columns);
}

export function typedSelect<T extends Record<string, any>>(): TypedSelectBuilder<T> {
    return new TypedSelectBuilder<T>();
}

// Export all types and classes
export {
    NuvqlSelect as Select,
    TypedSelectBuilder as TypedSelect,
    TypedEmbedBuilder as TypedEmbed
};
