import { PromiseResponseType } from "type";
import { BaseClient } from "../base-client";
import { TableQueryBuilder } from "./table";
import { DatabaseTypes } from "./types";

export class FnQueryBuilder<
  TClient extends BaseClient,
  TFunction extends DatabaseTypes.GenericFunction,
  TSchema extends DatabaseTypes.GenericSchema,
  TResult = TFunction["Returns"],
> extends TableQueryBuilder<TClient, any, TSchema, TResult> {
  constructor(
    private client: TClient,
    private config: { schema: string; functionName: string },
  ) {
    super(client, config as any);
  }

  async call(
    this: TFunction["Args"] extends Record<PropertyKey, never> ? this : never,
  ): PromiseResponseType<TClient, TResult>;
  async call(args: TFunction["Args"]): PromiseResponseType<TClient, TResult>;
  async call(args?: TFunction["Args"]): PromiseResponseType<TClient, TResult> {
    const url = new URL(
      this.client.config.endpoint +
        `/schemas/${this.config.schema}/fn/${this.config.functionName}`,
    );
    return this.client.withSafeResponse(() => {
      const headers = {
        "content-type": "application/json",
      };
      return this.client.call("POST", url, headers, args);
    });
  }

  // @ts-ignore
  override then<TResult1 = TResult, TResult2 = never>(
    onfulfilled?:
      | ((value: TResult) => TResult1 | PromiseLike<TResult1>)
      | undefined
      | null,
    onrejected?:
      | ((reason: any) => TResult2 | PromiseLike<TResult2>)
      | undefined
      | null,
  ) {
    // @ts-ignore
    return this.call().then(onfulfilled, onrejected);
  }

  protected execute(): never {
    throw new Error("use `.call()` method to execute the function call.");
  }

  join(): never {
    throw new Error("joining functions is not supported.");
  }

  single(): never {
    throw new Error("use `.call()` method to execute the function call.");
  }

  maybeSingle(): never {
    throw new Error("use `.call()` method to execute the function call.");
  }

  insert(): never {
    throw new Error("inserting into functions is not supported.");
  }

  update(): never {
    throw new Error("updating functions is not supported.");
  }

  delete(): never {
    throw new Error("deleting from functions is not supported.");
  }
}
