import type { BaseClient, Models } from "../base-client";
import { CollectionQueryBuilder } from "./collection";
import { TableQueryBuilder } from "./table";
import { DatabaseTypes } from "./types";

export class SchemaQueryBuilder<
  T extends BaseClient,
  Schema extends DatabaseTypes.GenericSchema,
  CollectionsTypes extends Record<string, Models.Document>,
> {
  constructor(
    private client: T,
    private schema: string,
  ) {}

  /**
   * It is used to query a collection in the database schema.
   * use `from` method instead, to query a table.
   * @param collectionId $id of collection
   *
   * @example
   * ```ts
   * import {BaseClient} from 'nuvix';
   *
   * const client = new BaseClient({ project: <project-id> })
   * const db = new Database(client)
   *
   * const collection = db.schema(<schema>).collection(<collectionId>)
   * // perform CRUD operations on the collection
   * ```
   */
  collection<Collection extends string & keyof CollectionsTypes>(collectionId: Collection) {
    return new CollectionQueryBuilder<T, Collection, CollectionsTypes>(
      this.client,
      { collectionId, schema: this.schema },
    );
  }

  /**
   * It is used to query a table in the database schema.
   * use `collection` method instead, to query a collection.
   *
   * @param tableName name of table
   *
   * @example
   * ```ts
   * import { BaseClient } from 'nuvix';
   *
   * const client = new BaseClient({ project: <project-id> })
   * const db = new Database(client)
   *
   * const table = db.schema(<schema>).from(<table>)
   * // perform CRUD operations on the table
   * ```
   */
  from<Table extends keyof Schema["Tables"] | keyof Schema["Views"]>(
    tableName: Table,
  ) {
    return new TableQueryBuilder<
      T,
      Table extends keyof Schema["Tables"]
        ? Schema["Tables"][Table]
        : Table extends keyof Schema["Views"]
          ? Schema["Views"][Table]
          : never,
      Schema
    >(this.client, {
      tableName: tableName as string,
      schema: this.schema,
    });
  }
}
