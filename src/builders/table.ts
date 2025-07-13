import type { Client } from "../client";

export class TableQueryBuilder<T extends Client, Table, SchemasTypes> {

    constructor(client: T, { }: { tableName: string, schema: string }) {

    }
}
