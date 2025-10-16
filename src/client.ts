import { Account } from "services/account";
import { BaseClient, Models } from "./base-client";
import { Avatars } from "services/avatars";
import { Locale } from "services/locale";
import { Messaging } from "services/messaging";
import { Storage } from "services/storage";
import { Teams } from "services/teams";
import { Database } from "services/database";
import { Schemas } from "type";

/**
 * Client
 *
 * The main entry point for interacting with Nuvix backend services.
 * Provides access to all available services (Account, Database, Messaging, Storage, Teams, etc.)
 * via lazy-loaded properties.
 *
 * Generics:
 * @template DB - The database schema type. Extends `Record<string, DatabaseTypes.GenericSchema>`.
 * @template CollectionsTypes - The collection document types. Defaults to DB if not specified.
 * @template IsSafe - Boolean flag controlling response type:
 *                     - true: Service/database methods return `{ error: boolean; data?: T }`.
 *                     - false: Methods return data directly (`T`).
 *
 * Example usage:
 *
 * // Safe response client with typed database
 * const client = new Client<MySchema, MySchema, true>({ safeResponse: true });
 * const result = await client.db.from('users').select('id', 'name');
 * if (!result.error) {
 *    console.log(result.data);
 * }
 *
 * // Unsafe response client (direct data)
 * const client2 = new Client<MySchema, MySchema, false>();
 * const users = await client2.db.from('users').select('id', 'name'); // users is directly typed array
 */
export class Client<
  DB extends Schemas,
  IsSafe extends boolean,
> extends BaseClient<IsSafe> {
  // Lazy private fields
  private _account?: Account<any>;
  private _avatars?: Avatars<any>;
  private _locale?: Locale<any>;
  private _messaging?: Messaging<any>;
  private _storage?: Storage<any>;
  private _teams?: Teams<any>;
  private _database?: Database<DB, this>;

  // Lazy getters
  public get account(): Account<BaseClient<IsSafe>> {
    if (!this._account) this._account = new Account(this);
    return this._account;
  }

  public get avatars(): Avatars<BaseClient<IsSafe>> {
    if (!this._avatars) this._avatars = new Avatars(this);
    return this._avatars;
  }

  public get locale(): Locale<BaseClient<IsSafe>> {
    if (!this._locale) this._locale = new Locale(this);
    return this._locale;
  }

  public get messaging(): Messaging<BaseClient<IsSafe>> {
    if (!this._messaging) this._messaging = new Messaging(this);
    return this._messaging;
  }

  public get storage(): Storage<BaseClient<IsSafe>> {
    if (!this._storage) this._storage = new Storage(this);
    return this._storage;
  }

  public get teams(): Teams<BaseClient<IsSafe>> {
    if (!this._teams) this._teams = new Teams(this);
    return this._teams;
  }

  public get database(): Database<DB, BaseClient<IsSafe>> {
    if (!this._database) this._database = new Database<DB, this>(this);
    return this._database;
  }

  public get db() {
    return this.database;
  }
}
