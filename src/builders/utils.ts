import { NuvixException } from "error";

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

  toString(): TAlias extends string
    ? TCast extends Cast
      ? `${TAlias}:${TColumn}::${TCast}`
      : `${TAlias}:${TColumn}`
    : TCast extends Cast
      ? `${TColumn}::${TCast}`
      : TColumn {
    let result = this._column as string;
    if (this._alias && typeof this._alias === "string") {
      result = `${this._alias}:${result}`;
    }
    if (this._castType && typeof this._castType === "string") {
      result = `${result}::${this._castType}`;
    }
    return result as any;
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

export function column<TColumn extends string>(
  name: TColumn,
): ColumnBuilder<TColumn, unknown, unknown, unknown> {
  return new ColumnBuilder(name);
}
