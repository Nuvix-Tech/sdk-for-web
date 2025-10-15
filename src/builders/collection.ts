import { PromiseResponseType } from "../type";
import type { BaseClient, Models, Query } from "../base-client";
import { Database } from "../services/database";

/**
 * A builder class for managing and interacting with a specific collection in a database.
 * Provides methods to perform CRUD operations on documents within the collection.
 *
 * @template T - The client type used for making API calls.
 * @template Collection - The type of the collection being managed.
 * @template CollectionsTypes - The type mapping for all collections in the database.
 */
export class CollectionQueryBuilder<
  T extends BaseClient,
  Collection,
  CollectionsTypes,
> {
  private collectionId: string;
  private schema: string;
  private db: Database<any, CollectionsTypes, T>;

  /**
   * Creates an instance of CollectionQueryBuilder.
   *
   * @param {T} client - The client instance used for API communication.
   * @param {Object} options - Options for initializing the builder.
   * @param {string} options.collectionId - The unique identifier of the collection.
   * @param {string} options.schema - The schema name associated with the collection.
   */
  constructor(
    client: T,
    { collectionId, schema }: { collectionId: string; schema: string },
  ) {
    this.collectionId = collectionId;
    this.schema = schema;
    this.db = new Database<any, CollectionsTypes, T>(client);
  }

  /**
   * Retrieves a list of documents from the collection based on the provided queries.
   *
   * @template Document - The type of the documents being retrieved.
   * @param {string[]} queries - Query parameters to filter the results.
   * @returns {PromiseResponseType<T, Models.DocumentList<Document>>} A promise resolving to a list of documents.
   */
  async find<Document extends Models.Document>(
    ...queries: string[]
  ): PromiseResponseType<T, Models.DocumentList<Document>> {
    return this.db.listDocuments<Document>(
      this.schema,
      this.collectionId,
      queries,
    );
  }

  /**
   * Retrieves a specific document from the collection by its unique ID.
   *
   * @template Document - The type of the document being retrieved.
   * @param {string} documentId - The unique identifier of the document.
   * @param {string[]} queries - Query parameters to filter the results.
   * @returns {PromiseResponseType<T, Document>} A promise resolving to the requested document.
   */
  async findById<Document extends Models.Document>(
    documentId: string,
    ...queries: string[]
  ): PromiseResponseType<T, Document> {
    return this.db.getDocument<Document>(
      this.schema,
      this.collectionId,
      documentId,
      queries,
    );
  }

  /**
   * Creates a new document in the collection.
   *
   * @template Document - The type of the document being created.
   * @param {string} documentId - The unique identifier for the new document.
   * @param {Omit<Document, keyof Models.Document>} data - The data for the new document, excluding system fields.
   * @param {string[]} [permissions] - Optional permissions for the document.
   * @returns {PromiseResponseType<T, Document>} A promise resolving to the created document.
   */
  async create<Document extends Models.Document>(
    documentId: string,
    data: Omit<Document, keyof Models.Document>,
    permissions?: string[],
  ): PromiseResponseType<T, Document> {
    return this.db.createDocument<Document>(
      this.schema,
      this.collectionId,
      documentId,
      data,
      permissions,
    );
  }

  /**
   * Updates an existing document in the collection.
   *
   * @template Document - The type of the document being updated.
   * @param {string} documentId - The unique identifier of the document to update.
   * @param {Partial<Omit<Document, keyof Models.Document>>} data - The partial data to update the document with.
   * @param {string[]} [permissions] - Optional permissions for the document.
   * @returns {PromiseResponseType<T, Document>} A promise resolving to the updated document.
   */
  async update<Document extends Models.Document>(
    documentId: string,
    data: Partial<Omit<Document, keyof Models.Document>>,
    permissions?: string[],
  ): PromiseResponseType<T, Document> {
    return this.db.updateDocument<Document>(
      this.schema,
      this.collectionId,
      documentId,
      data,
      permissions,
    );
  }

  /**
   * Deletes a document from the collection by its unique ID.
   *
   * @param {string} documentId - The unique identifier of the document to delete.
   * @returns {PromiseResponseType<T, {}>} A promise resolving to an empty object upon successful deletion.
   */
  async delete(documentId: string): PromiseResponseType<T, {}> {
    return this.db.deleteDocument(this.schema, this.collectionId, documentId);
  }
}
