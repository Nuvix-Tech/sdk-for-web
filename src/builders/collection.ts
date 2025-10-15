import { PromiseResponseType } from "../type";
import type { BaseClient, Models } from "../base-client";
import { Database } from "../services/database";
import { Query } from "../query";

/* ===========================
  Type Utilities (core)
  =========================== */

/** Base fields that are always present */
type BaseFields = Pick<Models.Document, "$sequence" | "$id" | "$collection" | "$schema" | "$createdAt" | "$updatedAt" | "$permissions">;

/** Custom attribute keys (excluding Models.Document base fields) */
type CustomKeys<Doc> = Exclude<Extract<keyof Doc, string>, keyof Models.Document>;

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
  Populated extends Record<string, any>
> = BaseFields &
  (Selected[number] extends "*"
   ? Pick<Doc, NonRelationKeys<Doc>>
   : Pick<Doc, Extract<Selected[number], NonRelationKeys<Doc>>>) &
  {
   [K in Extract<keyof Populated, RelationKeys<Doc>>]: Doc[K] extends Models.Document[]
   ? Populated[K][]
   : Populated[K];
  };

/** Populate all relations with their full types */
type PopulateAllRelations<Doc extends Models.Document> = {
  [K in RelationKeys<Doc>]: Doc[K] extends Models.Document[]
   ? BuildResultType<Doc[K][number], readonly ["*"], {}>[]
   : Doc[K] extends Models.Document
   ? BuildResultType<Doc[K], readonly ["*"], {}>
   : never;
};

/* ===========================
  Internal descriptor
  =========================== */

type Descriptor =
  | { kind: "query"; payload: string }
  | { kind: "select"; attributes: string[] | "*" }
  | { kind: "populate"; attr: string | "*"; nested: Descriptor[] };

/* ===========================
  Mini builder for populate callbacks
  =========================== */

class MiniBuilder<
  Doc extends Models.Document & Record<string, any>,
  CollectionsTypes extends Record<string, Models.Document & Record<string, any>>,
  Selected extends readonly string[] = readonly ["*"],
  Populated extends Record<string, any> = {}
> {
  readonly descriptors: Descriptor[];

  constructor(descriptors: Descriptor[] = []) {
   this.descriptors = [...descriptors];
  }

  private _clone<
   NewSelected extends readonly string[] = Selected,
   NewPopulated extends Record<string, any> = Populated
  >(desc: Descriptor[]): MiniBuilder<Doc, CollectionsTypes, NewSelected, NewPopulated> {
   return new MiniBuilder<Doc, CollectionsTypes, NewSelected, NewPopulated>(desc);
  }

  equal<K extends CustomKeys<Doc>>(
   attr: K,
   value: Doc[K]
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("equal", attr, value as any).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  notEqual<K extends CustomKeys<Doc>>(
   attr: K,
   value: Doc[K]
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("notEqual", attr, value as any).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  contains<K extends CustomKeys<Doc>>(
   attr: K,
   value: string | string[]
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("contains", attr, value as any).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  startsWith<K extends CustomKeys<Doc>>(
   attr: K,
   value: string
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("startsWith", attr, value).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  endsWith<K extends CustomKeys<Doc>>(
   attr: K,
   value: string
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("endsWith", attr, value).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderAsc<K extends CustomKeys<Doc>>(
   attr: K
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("orderAsc", attr).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderDesc<K extends CustomKeys<Doc>>(
   attr: K
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("orderDesc", attr).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  limit(n: number): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("limit", n).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  offset(n: number): MiniBuilder<Doc, CollectionsTypes, Selected, Populated> {
   const q = new Query("offset", n).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  select<K extends readonly (NonRelationKeys<Doc> | "*")[]>(
   ...attrs: K
  ): MiniBuilder<Doc, CollectionsTypes, K, Populated> {
   const mapped = attrs.length === 1 && attrs[0] === "*" ? "*" : (attrs as string[]);
   return this._clone<K, Populated>([...this.descriptors, { kind: "select", attributes: mapped }]);
  }

  populate<
    K extends RelationKeys<Doc> | '*',
    RelDoc extends K extends '*'
    ? never
    : ExtractRelationDoc<Doc[K]> = K extends '*'
    ? never
    : ExtractRelationDoc<Doc[K]>,
   NestedSelected extends readonly string[] = readonly ["*"],
   NestedPopulated extends Record<string, any> = {},
   NestedResult = BuildResultType<RelDoc, NestedSelected, NestedPopulated>
  >(
   attr: K,
   callback?: (
    b: MiniBuilder<RelDoc, CollectionsTypes>
   ) => MiniBuilder<RelDoc, CollectionsTypes, NestedSelected, NestedPopulated>
  ): MiniBuilder<Doc, CollectionsTypes, Selected, Populated & Record<K, NestedResult>> {
   const nestedBuilder = new MiniBuilder<RelDoc, CollectionsTypes>();
   const nested = callback ? callback(nestedBuilder).descriptors : [];
   return this._clone([...this.descriptors, { kind: "populate", attr: attr as string, nested }]);
  }
}

/* ===========================
  Main CollectionQueryBuilder
  =========================== */

export class CollectionQueryBuilder<
  T extends BaseClient,
  CollectionName extends keyof CollectionsTypes & string,
  CollectionsTypes extends Record<string, Models.Document & Record<string, unknown>>,
  Selected extends readonly string[] = readonly ["*"],
  Populated extends Record<string, any> = {}
> {
  private collectionId: CollectionName;
  private schema: string;
  private db: Database<any, CollectionsTypes, T>;
  private descriptors: Descriptor[] = [];

  constructor(
   client: T,
   opts: { collectionId: CollectionName; schema: string; descriptors?: Descriptor[] }
  ) {
   this.collectionId = opts.collectionId;
   this.schema = opts.schema;
   this.db = new Database<any, CollectionsTypes, T>(client);
   if (opts.descriptors) this.descriptors = [...opts.descriptors];
  }

  private _clone<
   NewSelected extends readonly string[] = Selected,
   NewPopulated extends Record<string, any> = Populated
  >(
   descriptors: Descriptor[]
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, NewSelected, NewPopulated> {
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

  equal<K extends CustomKeys<CollectionsTypes[CollectionName]>>(
   attr: K,
   value: CollectionsTypes[CollectionName][K]
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("equal", attr, value as any).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  notEqual<K extends CustomKeys<CollectionsTypes[CollectionName]>>(
   attr: K,
   value: CollectionsTypes[CollectionName][K]
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("notEqual", attr, value as any).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  contains<K extends CustomKeys<CollectionsTypes[CollectionName]>>(
   attr: K,
   value: string | string[]
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("contains", attr, value as any).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  startsWith<K extends CustomKeys<CollectionsTypes[CollectionName]>>(
   attr: K,
   value: string
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("startsWith", attr, value).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  endsWith<K extends CustomKeys<CollectionsTypes[CollectionName]>>(
   attr: K,
   value: string
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("endsWith", attr, value).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderAsc<K extends CustomKeys<CollectionsTypes[CollectionName]>>(
   attr: K
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("orderAsc", attr).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  orderDesc<K extends CustomKeys<CollectionsTypes[CollectionName]>>(
   attr: K
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("orderDesc", attr).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  limit(
   n: number
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("limit", n).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  offset(
   n: number
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, Selected, Populated> {
   const q = new Query("offset", n).toString();
   return this._clone([...this.descriptors, { kind: "query", payload: q }]);
  }

  /* =========================
    select
    ========================= */

  select<K extends readonly (NonRelationKeys<CollectionsTypes[CollectionName]> | "*")[]>(
   ...attributes: K
  ): CollectionQueryBuilder<T, CollectionName, CollectionsTypes, K, Populated> {
   const mapped =
    attributes.length === 1 && attributes[0] === "*" ? "*" : (attributes as string[]);
   return this._clone<K, Populated>([
    ...this.descriptors,
    { kind: "select", attributes: mapped },
   ]);
  }

  /* =========================
    populate - supports * and nested typing
    ========================= */

  populate<
   K extends RelationKeys<CollectionsTypes[CollectionName]> | '*',
   RelDoc extends K extends '*' 
    ? never 
    : ExtractRelationDoc<CollectionsTypes[CollectionName][K]> = K extends '*' 
    ? never 
    : ExtractRelationDoc<CollectionsTypes[CollectionName][K]>,
   NestedSelected extends readonly string[] = readonly ["*"],
   NestedPopulated extends Record<string, any> = {},
   NestedResult = K extends '*' 
    ? PopulateAllRelations<CollectionsTypes[CollectionName]>
    : BuildResultType<RelDoc, NestedSelected, NestedPopulated>
  >(
   attr: K,
   callback?: K extends '*' 
    ? never 
    : (b: MiniBuilder<RelDoc, CollectionsTypes>) => MiniBuilder<RelDoc, CollectionsTypes, NestedSelected, NestedPopulated>
  ): CollectionQueryBuilder<
   T,
   CollectionName,
   CollectionsTypes,
   Selected,
   K extends '*' ? (Populated & NestedResult) : (Populated & Record<K, NestedResult>)
  > {
   if (attr === '*') {
    return this._clone([
      ...this.descriptors,
      { kind: "populate", attr: "*", nested: [] },
    ]);
   }

   const nestedBuilder = new MiniBuilder<RelDoc, CollectionsTypes>();
   const nested = callback ? callback(nestedBuilder).descriptors : [];

   return this._clone([
    ...this.descriptors,
    { kind: "populate", attr: attr as string, nested },
   ]);
  }

  /* =========================
    Serialization
    ========================= */

  private buildQueryStrings(): string[] {
   return this.processDescriptors(this.descriptors);
  }

  private processDescriptors(descriptors: Descriptor[]): string[] {
   const out: string[] = [];

   for (const d of descriptors) {
    if (d.kind === "query") {
      out.push(d.payload);
    } else if (d.kind === "select") {
      const q = new Query("select", d.attributes === "*" ? "*" : d.attributes).toString();
      out.push(q);
    } else if (d.kind === "populate") {
      if (d.attr === "*") {
       const q = new Query("populate", "*").toString();
       out.push(q);
      } else {
       const nestedStrings = this.processDescriptors(d.nested);
       const nestedParsed = nestedStrings.map((s) => {
        try {
          return JSON.parse(s);
        } catch {
          return s;
        }
       });
       const q = new Query("populate", d.attr, nestedParsed.length > 0 ? nestedParsed : undefined).toString();
       out.push(q);
      }
    }
   }

   return out;
  }

  /* =========================
    Execution methods
    ========================= */

  async find(): PromiseResponseType<
   T,
   Models.DocumentList<BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>>
  > {
   const qs = this.buildQueryStrings();
   return this.db.listDocuments<
    BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>
   >(this.schema, String(this.collectionId), qs);
  }

  async findById(
   id: string
  ): PromiseResponseType<
   T,
   BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>
  > {
   const qs = this.buildQueryStrings();
   return this.db.getDocument<
    BuildResultType<CollectionsTypes[CollectionName], Selected, Populated>
   >(this.schema, String(this.collectionId), id, qs);
  }

  async create<Document extends CollectionsTypes[CollectionName]>(
   documentId: string,
   data: Omit<Document, keyof Models.Document>,
   permissions?: string[]
  ): PromiseResponseType<T, Document> {
   return this.db.createDocument<Document>(
    this.schema,
    String(this.collectionId),
    documentId,
    data,
    permissions
   );
  }

  async update<Document extends CollectionsTypes[CollectionName]>(
   documentId: string,
   data: Partial<Omit<Document, keyof Models.Document>>,
   permissions?: string[]
  ): PromiseResponseType<T, Document> {
   return this.db.updateDocument<Document>(
    this.schema,
    String(this.collectionId),
    documentId,
    data,
    permissions
   );
  }

  async delete(documentId: string): PromiseResponseType<T, {}> {
   return this.db.deleteDocument(this.schema, String(this.collectionId), documentId);
  }
}
