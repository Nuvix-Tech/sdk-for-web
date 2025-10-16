import { Schemas as _Schemas, GetSchemaType, GetTableOrView } from "type";
import type { BaseClient } from "../base-client";
import { CollectionQueryBuilder } from "./collection";
import { TableQueryBuilder } from "./table";

export class SchemaQueryBuilder<
  T extends BaseClient,
  Schema extends keyof Schemas,
  Schemas extends _Schemas,
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
  collection<
    Collection extends string & keyof GetSchemaType<Schemas, Schema, true>,
  >(collectionId: Collection) {
    return new CollectionQueryBuilder<
      T,
      Collection,
      GetSchemaType<Schemas, Schema, true>
    >(this.client, { collectionId, schema: this.schema });
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
  from<
    Table extends
      | keyof GetSchemaType<Schemas, Schema, false>["Tables"]
      | keyof GetSchemaType<Schemas, Schema, false>["Views"],
  >(tableName: Table) {
    return new TableQueryBuilder<
      T,
      GetTableOrView<Schemas, Schema, Table>,
      GetSchemaType<Schemas, Schema, false>
    >(this.client, {
      tableName: tableName as string,
      schema: this.schema,
    });
  }
}
