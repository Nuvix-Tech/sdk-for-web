import { Schemas as _Schemas, GetSchemaType, GetTableOrView } from "../type";
import type { BaseClient } from "../base-client";
import { CollectionQueryBuilder } from "./collection";
import { TableQueryBuilder } from "./table";
import { FnQueryBuilder } from "./fn";

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
   * import {Client} from 'nuvix';
   *
   * const nx = new Client({ project: <project-id> })
   *
   * const collection = nx.db.schema(<schema>).collection(<collectionId>)
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
   * import { Client } from 'nuvix';
   *
   * const nx = new Client({ project: <project-id> })
   *
   * const table = nx.db.schema(<schema>).from(<table>)
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

  /**
   * It is used to call a database function (RPC) in the database schema.
   *
   * @param fn name of function
   *
   * @example
   * ```ts
   * import { Client } from 'nuvix';
   *
   * const nx = new Client({ project: <project-id> })
   *
   * const result = await nx.db.schema(<schema>).fn(<function-name>).call(<args>)
   * ```
   */
  fn<
    Fn extends string &
      keyof GetSchemaType<Schemas, Schema, false>["Functions"],
  >(fn: Fn) {
    return new FnQueryBuilder<
      T,
      GetSchemaType<Schemas, Schema, false>["Functions"][Fn],
      GetSchemaType<Schemas, Schema, false>
    >(this.client, {
      schema: this.schema,
      functionName: fn,
    });
  }

  /**
   * Alias for `fn` method.
   */
  readonly rpc = this.fn;
}
