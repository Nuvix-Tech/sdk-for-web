import { PromiseResponseType } from "../type";
import type { BaseClient, Models } from "../base-client";
import { Database } from "../services/database";
import { Query } from "../query";

/* ===========================
  Type Utilities (core)
  =========================== */

/** Base fields that are always present */
type BaseFields = Pick<
  Models.Document,
  "$sequence" | "$id" | "$collection" | "$schema" | "$permissions"
>;

/** Extract custom attribute keys (excluding Models.Document base fields) */
type CustomKeys<Doc> = Exclude<Extract<keyof Doc, string>, keyof BaseFields>;

/** Keys whose value type extends Models.Document or Models.Document[] */
type RelationKeys<Doc> = {
  [K in CustomKeys<Doc>]: NonNullable<Doc[K]> extends Models.Document
    ? K
    : NonNullable<Doc[K]> extends Models.Document[]
      ? K
      : never;
}[CustomKeys<Doc>];

/** Non-relation custom keys */
type NonRelationKeys<Doc> = Exclude<CustomKeys<Doc>, RelationKeys<Doc>>;

type SelectionKeys<Doc> = NonRelationKeys<Doc> | "$createdAt" | "$updatedAt";

/** Extract document type from a relation field */
type ExtractRelationDoc<T> = T extends Models.Document[]
  ? T[number]
  : T extends Models.Document
    ? T
    : never;

/** Build result type based on selections and populations */
type BuildResultType<
  Doc extends Models.Document,
  Selected extends readonly string[],
  Populated extends Record<string, any>,
> = BaseFields &
  // Include selected non-relation fields
  (Selected[number] extends "*"
    ? Pick<Doc, NonRelationKeys<Doc>>
    : Pick<Doc, Extract<Selected[number], NonRelationKeys<Doc>>>) &
  // Include populated relations
  {
    [K in keyof Populated]: K extends RelationKeys<Doc>
      ? Doc[K] extends Models.Document[]
        ? Populated[K][]
        : Populated[K]
      : never;
  };

/** Populate all relations with their full types */
type PopulateAllRelations<Doc extends Models.Document> = {
  [K in RelationKeys<Doc>]: Doc[K] extends Models.Document[]
    ? BuildResultType<ExtractRelationDoc<Doc[K]>, readonly ["*"], {}>
    : Doc[K] extends Models.Document
      ? BuildResultType<ExtractRelationDoc<Doc[K]>, readonly ["*"], {}>
      : never;
};

type FilterKeys<Doc> =
  | NonRelationKeys<Doc>
  | "$id"
  | "$updatedAt"
  | "$createdAt"
  | "$sequence";

/* ===========================
  Internal descriptor
  =========================== */

type Descriptor =
  | { kind: "query"; payload: string }
  | { kind: "select"; attributes: string[] | "*" }
  | { kind: "populate"; attr: string; nested: Descriptor[] }
  | { kind: "populateAll" };

/* ===========================
  Mini builder for populate callbacks
  =========================== */

class MiniBuilder<
  Doc extends Models.Document,
  CollectionsTypes extends Record<string, Models.Document>,
  Selected extends readonly string[] = readonly ["*"],
  Populated extends Record<string, any> = {},
> {
  readonly descriptors: Descriptor[];

  constructor(descriptors: Descriptor[] = []) {
    this.descriptors = [...descriptors];
  }

  private _clone<
    NewSelected extends readonly string[] = Selected,
    NewPopulated extends Record<string, any> = Populated,
  >(
    desc: Descriptor[],
  ): MiniBuilder<Doc, CollectionsTypes, NewSelected, NewPopulated> {
    return new MiniBuilder<Doc, CollectionsTypes, NewSelected, NewPopulated>(
      desc,
    );
  }

  // Query methods (all except limit & offset as per your requirement)
  equal<K extends FilterKeys<Doc>>(
    attr: K,
    value: Doc[K],
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
    const q = new Query("equal", attr, value as any).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  notEqual<K extends FilterKeys<Doc>>(
    attr: K,
    value: Doc[K],
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
    const q = new Query("notEqual", attr, value as any).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  contains<K extends FilterKeys<Doc>>(
    attr: K,
    value: string | string[],
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
    const q = new Query("contains", attr, value as any).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  startsWith<K extends FilterKeys<Doc>>(
    attr: K,
    value: string,
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
    const q = new Query("startsWith", attr, value).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  endsWith<K extends FilterKeys<Doc>>(
    attr: K,
    value: string,
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
    const q = new Query("endsWith", attr, value).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderAsc<K extends FilterKeys<Doc>>(
    attr: K,
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
    const q = new Query("orderAsc", attr).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderDesc<K extends FilterKeys<Doc>>(
    attr: K,
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
    const q = new Query("orderDesc", attr).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  select<K extends readonly (SelectionKeys<Doc> | "*")[]>(
    ...attrs: K
  ): MiniBuilder<Doc, CollectionsTypes, K, Populated> {
    const mapped =
      attrs.length === 1 && attrs[0] === "*"
        ? "*"
        : (attrs as unknown as string[]);
    return this._clone<K, Populated>([
      ...this.descriptors,
      { kind: "select", attributes: mapped },
    ]);
  }

  populate<
    K extends RelationKeys<Doc>,
    RelDoc extends ExtractRelationDoc<Doc[K]> = ExtractRelationDoc<Doc[K]>,
    NestedSelected extends readonly string[] = readonly ["*"],
    NestedPopulated extends Record<string, any> = {},
  >(
    attr: K,
    callback?: (
      b: MiniBuilder<RelDoc, CollectionsTypes>,
    ) => MiniBuilder<RelDoc, CollectionsTypes, NestedSelected, NestedPopulated>,
  ): MiniBuilder<
    Doc,
    CollectionsTypes,
    Selected,
    Populated &
      Record<K, BuildResultType<RelDoc, NestedSelected, NestedPopulated>>
  > {
    const nestedBuilder = new MiniBuilder<RelDoc, CollectionsTypes>();
    const nested = callback ? callback(nestedBuilder).descriptors : [];
    return this._clone([
      ...this.descriptors,
      { kind: "populate", attr: attr as string, nested },
    ]);
  }

  /* =========================
  populateAll - populate all relations with full fields
  ========================= */

  populateAll(): MiniBuilder<
    Doc,
    CollectionsTypes,
    Selected,
    Populated & PopulateAllRelations<Doc>
  > {
    return this._clone([...this.descriptors, { kind: "populateAll" }]);
  }
}

/* ===========================
  Main CollectionQueryBuilder
  =========================== */

export class CollectionQueryBuilder<
  T extends BaseClient,
  CollectionName extends keyof CollectionsTypes & string,
  CollectionsTypes extends Record<string, Models.Document>,
  Selected extends readonly string[] = readonly ["*"],
  Populated extends Record<string, any> = {},
> {
  private collectionId: CollectionName;
  private schema: string;
  private db: Database<any, T>;
  private descriptors: Descriptor[] = [];

  constructor(
    client: T,
    opts: {
      collectionId: CollectionName;
      schema: string;
      descriptors?: Descriptor[];
    },
  ) {
    this.collectionId = opts.collectionId;
    this.schema = opts.schema;
    this.db = new Database<any, T>(client);
    if (opts.descriptors) this.descriptors = [...opts.descriptors];
  }

  private _clone<
    NewSelected extends readonly string[] = Selected,
    NewPopulated extends Record<string, any> = Populated,
  >(
    descriptors: Descriptor[],
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    NewSelected,
    NewPopulated
  > {
    return new CollectionQueryBuilder<
      T,
      CollectionName,
      CollectionsTypes,
      NewSelected,
      NewPopulated
    >(this.db["client"] as T, {
      collectionId: this.collectionId,
      schema: this.schema,
      descriptors,
    });
  }

  /* =========================
    Query methods
    ========================= */

  equal<K extends FilterKeys<CollectionsTypes[CollectionName]>>(
    attr: K,
    value: CollectionsTypes[CollectionName][K],
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = new Query("equal", attr, value as any).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  notEqual<K extends FilterKeys<CollectionsTypes[CollectionName]>>(
    attr: K,
    value: CollectionsTypes[CollectionName][K],
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = new Query("notEqual", attr, value as any).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  contains<K extends FilterKeys<CollectionsTypes[CollectionName]>>(
    attr: K,
    value: string | string[],
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = new Query("contains", attr, value as any).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  startsWith<K extends FilterKeys<CollectionsTypes[CollectionName]>>(
    attr: K,
    value: string,
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = new Query("startsWith", attr, value).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  endsWith<K extends FilterKeys<CollectionsTypes[CollectionName]>>(
    attr: K,
    value: string,
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = new Query("endsWith", attr, value).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderAsc<K extends FilterKeys<CollectionsTypes[CollectionName]>>(
    attr: K,
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = Query.orderAsc(attr).toString();
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderDesc<K extends FilterKeys<CollectionsTypes[CollectionName]>>(
    attr: K,
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = Query.orderDesc(attr);
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  limit(
    n: number,
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = Query.limit(n);
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  offset(
    n: number,
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated
  > {
    const q = Query.offset(n);
    return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  /* =========================
    select
    ========================= */

  select<
    K extends readonly (
      | SelectionKeys<CollectionsTypes[CollectionName]>
      | "*"
    )[],
  >(
    ...attributes: K
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, K, Populated> {
    const mapped =
      attributes.length === 1 && attributes[0] === "*"
        ? "*"
        : (attributes as unknown as string[]);
    return this._clone<K, Populated>([
      ...this.descriptors,
      { kind: "select", attributes: mapped },
    ]);
  }

  /* =========================
    populate - single relation with nested queries
    ========================= */

  populate<
    K extends RelationKeys<CollectionsTypes[CollectionName]>,
    RelDoc extends ExtractRelationDoc<
      CollectionsTypes[CollectionName][K]
    > = ExtractRelationDoc<CollectionsTypes[CollectionName][K]>,
    NestedSelected extends readonly string[] = readonly ["*"],
    NestedPopulated extends Record<string, any> = {},
  >(
    attr: K,
    callback?: (
      b: MiniBuilder<RelDoc, CollectionsTypes>,
    ) => MiniBuilder<RelDoc, CollectionsTypes, NestedSelected, NestedPopulated>,
  ): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated &
      Record<K, BuildResultType<RelDoc, NestedSelected, NestedPopulated>>
  > {
    const nestedBuilder = new MiniBuilder<RelDoc, CollectionsTypes>();
    const nested = callback ? callback(nestedBuilder).descriptors : [];

    return this._clone([
      ...this.descriptors,
      { kind: "populate", attr: attr as string, nested },
    ]);
  }

  /* =========================
    populateAll - populate all relations with full fields
    ========================= */

  populateAll(): CollectionQueryBuilder<
    T,
    CollectionName,
    CollectionsTypes,
    Selected,
    Populated & PopulateAllRelations<CollectionsTypes[CollectionName]>
  > {
    return this._clone([...this.descriptors, { kind: "populateAll" }]);
  }

  /* =========================
    Serialization - FIXED for proper query building
    ========================= */

  private buildQueryStrings(): string[] {
    return this.processDescriptors(this.descriptors);
  }

  private processDescriptors(descriptors: Descriptor[]): string[] {
    const queries: string[] = [];
    const selectQueries: string[] = [];
    const populateQueries: string[] = [];

    // Process all descriptors and group by type
    for (const d of descriptors) {
      switch (d.kind) {
        case "query":
          queries.push(d.payload);
          break;
        case "select":
          const selectAttr = d.attributes === "*" ? "*" : d.attributes;
          selectQueries.push(
            new Query("select", undefined, selectAttr).toString(),
          );
          break;
        case "populate":
          const nestedQueries = this.processDescriptors(d.nested);
          // For populate queries, we need to build the nested structure
          if (nestedQueries.length > 0) {
            const nestedParsed = nestedQueries.map((s) => {
              try {
                return JSON.parse(s);
              } catch {
                return s;
              }
            });
            populateQueries.push(
              new Query("populate", d.attr, nestedParsed).toString(),
            );
          } else {
            populateQueries.push(new Query("populate", d.attr).toString());
          }
          break;
        case "populateAll":
          populateQueries.push(new Query("populate", "*").toString());
          break;
      }
    }

    // Combine all queries: regular queries first, then select, then populate
    return [...queries, ...selectQueries, ...populateQueries];
  }

  /* =========================
    Execution methods
    ========================= */

  async find(): PromiseResponseType<
    T,
    Models.DocumentList<
      // @ts-ignore
      BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>
    >
  > {
    const qs = this.buildQueryStrings();
    return this.db.listDocuments<
      // @ts-ignore
      BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>
    >(this.schema, String(this.collectionId), qs);
  }

  async findById(
    id: string,
  ): PromiseResponseType<
    T,
    BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>
  > {
    const qs = this.buildQueryStrings();
    return this.db.getDocument<
      // @ts-ignore
      BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>
    >(this.schema, String(this.collectionId), id, qs);
  }

  async create<Document extends CollectionsTypes[CollectionName]>(
    documentId: string,
    data: Omit<Document, keyof Models.Document>,
    permissions?: string[],
  ): PromiseResponseType<T, Document> {
    return this.db.createDocument<Document>(
      this.schema,
      String(this.collectionId),
      documentId,
      data,
      permissions,
    );
  }

  async update<Document extends CollectionsTypes[CollectionName]>(
    documentId: string,
    data: Partial<Omit<Document, keyof Models.Document>>,
    permissions?: string[],
  ): PromiseResponseType<T, Document> {
    return this.db.updateDocument<Document>(
      this.schema,
      String(this.collectionId),
      documentId,
      data,
      permissions,
    );
  }

  async delete(documentId: string): PromiseResponseType<T, {}> {
    return this.db.deleteDocument(
      this.schema,
      String(this.collectionId),
      documentId,
    );
  }
}
