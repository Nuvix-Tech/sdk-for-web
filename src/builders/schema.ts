import type { Client } from "../client";
import { CollectionQueryBuilder } from "./collection";
import { TableQueryBuilder } from "./table";

export class SchemaQueryBuilder<T extends Client, SchemasTypes, CollectionsTypes> {
    constructor(private client: T, private schema: string) { }

    /**
     * It is used to query a collection in the database schema.
     * use `from` method instead, to query a table.
     * @param collectionId $id of collection
     * 
     * @example 
     * ```ts
     * import {Client} from 'nuvix';
     * 
     * const client = new Client({ project: <project-id> })
     * const db = new Database(client)
     * 
     * const collection = db.schema(<schema>).collection(<collectionId>)
     * // perform CRUD operations on the collection
     * ```
     */
    collection<Schema = unknown>(collectionId: string) {
        return new CollectionQueryBuilder<T, Schema, CollectionsTypes>(this.client, { collectionId, schema: this.schema });
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
     * const client = new Client({ project: <project-id> })
     * const db = new Database(client)
     * 
     * const table = db.schema(<schema>).from(<table>)
     * // perform CRUD operations on the table
     * ```
     */
    from<Table = unknown>(tableName: string) {
        return new TableQueryBuilder<T, Table, SchemasTypes>(this.client, { tableName, schema: this.schema });
    }
}
