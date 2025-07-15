export const cast = ['text', 'integere'] as const;
export type Cast = typeof cast[number]

export class ColumnBuilder<C extends string, A extends unknown, CA extends unknown = Cast> {
    private _as?: string;
    private _cast?: string;

    constructor(private col: C) { }

    as<_A extends string>(as: _A): ColumnBuilder<C, _A, CA> {
        this._as = as
        return this as any;
    }

    cast<_CA extends string>(cast: _CA): ColumnBuilder<C, A, _CA> {
        this._cast = cast
        return this as any;
    }

    toString(): A extends string ?
        CA extends Cast ? `${A}:${C}::${CA}` : `${A}:${C}` : CA extends Cast ? `${C}::${CA}` : C {
        const _as = this._as !== undefined ? `${this._as}:` : '';
        const _cast = this._cast !== undefined ? `::${this._cast}` : '';
        return `${_as}${this.col}${_cast}` as any;
    }
}


export function column<T extends string>(col: T) {
    return new ColumnBuilder(col);
}

export type Column<T extends string, A extends unknown, Ca extends Cast = Cast> = ColumnBuilder<T, A, Ca>;
