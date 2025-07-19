import { Column, ColumnBuilder } from "builders/utils";

function funcToString(func: string, selector: string) {
  return `${func}(${selector})`;
}

/**
 * Calculates the sum of all values in the specified column.
 *
 * @example
 * .select(sum('price'))
 * .select(sum('quantity').alias('total_quantity'))
 */
export function sum<TColumn extends string>(
  name: TColumn,
): ColumnBuilder<TColumn, "sum", "int", unknown> {
  return new Column(funcToString("sum", name)).as("sum") as any;
}

/**
 * Calculates the average value of all values in the specified column.
 *
 * @example
 * .select(avg('price'))
 * .select(avg('rating').alias('average_rating'))
 */
export function avg<TColumn extends string>(
  name: TColumn,
): ColumnBuilder<TColumn, "avg", "float", unknown> {
  return new Column(funcToString("avg", name)).as("avg") as any;
}

/**
 * Counts the number of rows that have non-null values in the specified column.
 * When used with '*', counts all rows in the result set.
 *
 * @example
 * .select(count('*'))
 * .select(count('user_id'))
 * .select(count('email').alias('total_users'))
 */
export function count<TColumn extends string | "*">(
  name: TColumn | "*",
): ColumnBuilder<TColumn extends "*" ? any : TColumn, "count", "int", unknown> {
  return new Column(funcToString("count", name)).as("count") as any;
}

/**
 * Finds the maximum value in the specified column.
 * Can be used with numeric, date, or string columns to find the highest value.
 *
 * @example
 * .select(max('price'))
 * .select(max('created_at'))
 * .select(max('score').alias('highest_score'))
 */
export function max<TColumn extends string>(
  name: TColumn,
): ColumnBuilder<TColumn, "max", TColumn, unknown> {
  return new Column(funcToString("max", name)).as("max") as any;
}

/**
 * Finds the minimum value in the specified column.
 * Can be used with numeric, date, or string columns to find the lowest value.
 *
 * @example
 * .select(min('price'))
 * .select(min('created_at'))
 * .select(min('score').alias('lowest_score'))
 */
export function min<TColumn extends string>(
  name: TColumn,
): ColumnBuilder<TColumn, "min", TColumn, unknown> {
  return new Column(funcToString("min", name)).as("min") as any;
}
