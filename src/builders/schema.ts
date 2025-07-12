import type { Client } from "../client";
import { CollectionQueryBuilder } from "./collection";
import { TableQueryBuilder } from "./table";

export class SchemaQueryBuilder<T extends Client, SchemasTypes, CollectionsTypes> {
    constructor(private client: T, private schema: string) { }

    /**
     * 
     * @param collectionId $id of collection
     */
    collection<Schema = any>(collectionId: string) {
        return new CollectionQueryBuilder<T, Schema, CollectionsTypes>(this.client, { collectionId, schema: this.schema });
    }

    /**
     * 
     * @param tableName name of table
     */
    from<Table = any>(tableName: string) {
        return new TableQueryBuilder<T, Table, SchemasTypes>(this.client, { tableName, schema: this.schema });
    }
}
