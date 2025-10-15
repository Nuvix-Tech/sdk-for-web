import { PromiseResponseType } from "../type";
import { NuvixException, BaseClient, type Payload } from "../base-client";
import type { Models } from "../models";
import { SchemaQueryBuilder } from "../builders/schema";
import { DatabaseTypes } from "builders/types";
import { TableQueryBuilder } from "builders";

export class Database<
  Database extends Record<string, DatabaseTypes.GenericSchema>,
  CollectionsTypes extends Record<string, Models.Document>,
  T extends BaseClient,
> {
  client: T;

  constructor(client: T) {
    this.client = client;
  }

  /**
   *
   * @param schema string
   */
  schema<Schema extends keyof Database>(schema: Schema) {
    return new SchemaQueryBuilder<T, Database[Schema], CollectionsTypes>(
      this.client,
      schema as string,
    );
  }

  /**
   * Creates a query builder for interacting with a specific table or view in the "public" schema of the database.
   *
   * @template Table - The name of the table or view in the "public" schema. This can be a key of either
   * `Database['public']["Tables"]` or `Database['public']["Views"]`.
   *
   * @param tableName - The name of the table or view to query. Must exist in the "public" schema.
   *
   * @returns A `TableQueryBuilder` instance configured for the specified table or view.
   *
   * @example
   * ```typescript
   * const queryBuilder = db.from('users');
   * ```
   */
  from<
    Table extends
      | keyof Database["public"]["Tables"]
      | keyof Database["public"]["Views"],
  >(tableName: Table) {
    return new TableQueryBuilder<
      T,
      Table extends keyof Database["public"]["Tables"]
        ? Database["public"]["Tables"][Table]
        : Table extends keyof Database["public"]["Views"]
          ? Database["public"]["Views"][Table]
          : never,
      Database["public"]
    >(this.client, {
      tableName: tableName as string,
      schema: "public",
    });
  }

  /**
   * List documents
   *
   * Get a list of all the user&#039;s documents in a given collection. You can use the query params to filter your results.
   *
   * @param {string} databaseId
   * @param {string} collectionId
   * @param {string[]} queries
   * @returns {PromiseResponseType<T, Models.DocumentList<Document>>}
   */
  async listDocuments<Document extends Models.Document>(
    databaseId: string,
    collectionId: string,
    queries?: string[],
  ): PromiseResponseType<T, Models.DocumentList<Document>> {
    return this.client.withSafeResponse(async () => {
      if (typeof databaseId === "undefined") {
        throw new NuvixException('Missing required parameter: "databaseId"');
      }
      if (typeof collectionId === "undefined") {
        throw new NuvixException('Missing required parameter: "collectionId"');
      }
      const apiPath =
        "/schemas/{databaseId}/collections/{collectionId}/documents"
          .replace("{databaseId}", databaseId)
          .replace("{collectionId}", collectionId);
      const payload: Payload = {};
      if (typeof queries !== "undefined") {
        payload["queries"] = queries;
      }
      const uri = new URL(this.client.config.endpoint + apiPath);

      const apiHeaders: { [header: string]: string } = {
        "content-type": "application/json",
      };

      return await this.client.call("get", uri, apiHeaders, payload);
    });
  }
  /**
   * Create document
   *
   * Create a new Document. Before using this route, you should create a new collection resource using either a [server integration](https://nuvix.io/docs/server/schemas#databasesCreateCollection) API or directly from your database console.
   *
   * @param {string} databaseId
   * @param {string} collectionId
   * @param {string} documentId
   * @param {Omit<Document, keyof Models.Document>} data
   * @param {string[]} permissions
   * @returns {PromiseResponseType<T, Document>}
   */
  async createDocument<Document extends Models.Document>(
    databaseId: string,
    collectionId: string,
    documentId: string,
    data: Omit<Document, keyof Models.Document>,
    permissions?: string[],
  ): PromiseResponseType<T, Document> {
    return this.client.withSafeResponse(async () => {
      if (typeof databaseId === "undefined") {
        throw new NuvixException('Missing required parameter: "databaseId"');
      }
      if (typeof collectionId === "undefined") {
        throw new NuvixException('Missing required parameter: "collectionId"');
      }
      if (typeof documentId === "undefined") {
        throw new NuvixException('Missing required parameter: "documentId"');
      }
      if (typeof data === "undefined") {
        throw new NuvixException('Missing required parameter: "data"');
      }
      const apiPath =
        "/schemas/{databaseId}/collections/{collectionId}/documents"
          .replace("{databaseId}", databaseId)
          .replace("{collectionId}", collectionId);
      const payload: Payload = {};
      if (typeof documentId !== "undefined") {
        payload["documentId"] = documentId;
      }
      if (typeof data !== "undefined") {
        payload["data"] = data;
      }
      if (typeof permissions !== "undefined") {
        payload["permissions"] = permissions;
      }
      const uri = new URL(this.client.config.endpoint + apiPath);

      const apiHeaders: { [header: string]: string } = {
        "content-type": "application/json",
      };

      return await this.client.call("post", uri, apiHeaders, payload);
    });
  }
  /**
   * Get document
   *
   * Get a document by its unique ID. This endpoint response returns a JSON object with the document data.
   *
   * @param {string} databaseId
   * @param {string} collectionId
   * @param {string} documentId
   * @param {string[]} queries
   * @returns {PromiseResponseType<T, Document>}
   */
  async getDocument<Document extends Models.Document>(
    databaseId: string,
    collectionId: string,
    documentId: string,
    queries?: string[],
  ): PromiseResponseType<T, Document> {
    return this.client.withSafeResponse(async () => {
      if (typeof databaseId === "undefined") {
        throw new NuvixException('Missing required parameter: "databaseId"');
      }
      if (typeof collectionId === "undefined") {
        throw new NuvixException('Missing required parameter: "collectionId"');
      }
      if (typeof documentId === "undefined") {
        throw new NuvixException('Missing required parameter: "documentId"');
      }
      const apiPath =
        "/schemas/{databaseId}/collections/{collectionId}/documents/{documentId}"
          .replace("{databaseId}", databaseId)
          .replace("{collectionId}", collectionId)
          .replace("{documentId}", documentId);
      const payload: Payload = {};
      if (typeof queries !== "undefined") {
        payload["queries"] = queries;
      }
      const uri = new URL(this.client.config.endpoint + apiPath);

      const apiHeaders: { [header: string]: string } = {
        "content-type": "application/json",
      };

      return await this.client.call("get", uri, apiHeaders, payload);
    });
  }
  /**
   * Update document
   *
   * Update a document by its unique ID. Using the patch method you can pass only specific fields that will get updated.
   *
   * @param {string} databaseId
   * @param {string} collectionId
   * @param {string} documentId
   * @param {Partial<Omit<Document, keyof Models.Document>>} data
   * @param {string[]} permissions
   * @returns {PromiseResponseType<T, Document>}
   */
  async updateDocument<Document extends Models.Document>(
    databaseId: string,
    collectionId: string,
    documentId: string,
    data?: Partial<Omit<Document, keyof Models.Document>>,
    permissions?: string[],
  ): PromiseResponseType<T, Document> {
    return this.client.withSafeResponse(async () => {
      if (typeof databaseId === "undefined") {
        throw new NuvixException('Missing required parameter: "databaseId"');
      }
      if (typeof collectionId === "undefined") {
        throw new NuvixException('Missing required parameter: "collectionId"');
      }
      if (typeof documentId === "undefined") {
        throw new NuvixException('Missing required parameter: "documentId"');
      }
      const apiPath =
        "/schemas/{databaseId}/collections/{collectionId}/documents/{documentId}"
          .replace("{databaseId}", databaseId)
          .replace("{collectionId}", collectionId)
          .replace("{documentId}", documentId);
      const payload: Payload = {};
      if (typeof data !== "undefined") {
        payload["data"] = data;
      }
      if (typeof permissions !== "undefined") {
        payload["permissions"] = permissions;
      }
      const uri = new URL(this.client.config.endpoint + apiPath);

      const apiHeaders: { [header: string]: string } = {
        "content-type": "application/json",
      };

      return await this.client.call("patch", uri, apiHeaders, payload);
    });
  }
  /**
   * Delete document
   *
   * Delete a document by its unique ID.
   *
   * @param {string} databaseId
   * @param {string} collectionId
   * @param {string} documentId
   * @returns {PromiseResponseType<T, {}>}
   */
  async deleteDocument(
    databaseId: string,
    collectionId: string,
    documentId: string,
  ): PromiseResponseType<T, {}> {
    return this.client.withSafeResponse(async () => {
      if (typeof databaseId === "undefined") {
        throw new NuvixException('Missing required parameter: "databaseId"');
      }
      if (typeof collectionId === "undefined") {
        throw new NuvixException('Missing required parameter: "collectionId"');
      }
      if (typeof documentId === "undefined") {
        throw new NuvixException('Missing required parameter: "documentId"');
      }
      const apiPath =
        "/schemas/{databaseId}/collections/{collectionId}/documents/{documentId}"
          .replace("{databaseId}", databaseId)
          .replace("{collectionId}", collectionId)
          .replace("{documentId}", documentId);
      const payload: Payload = {};
      const uri = new URL(this.client.config.endpoint + apiPath);

      const apiHeaders: { [header: string]: string } = {
        "content-type": "application/json",
      };

      return await this.client.call("delete", uri, apiHeaders, payload);
    });
  }
}
