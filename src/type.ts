import { DatabaseTypes } from "./builders/types";
import type { BaseClient, Models } from "./base-client";
import { NuvixException } from "./error";
import { Socket } from "socket.io-client";

/**
 * Payload type representing a key-value pair with string keys and any values.
 */
export type Payload = {
  [key: string]: any;
};

/**
 * Headers type representing a key-value pair with string keys and string values.
 */
export type Headers = {
  [key: string]: string;
};

/**
 * Realtime response structure with different types.
 */
export type RealtimeResponse = {
  /**
   * Type of the response: 'error', 'event', 'connected', 'response' or 'pong'.
   */
  type: "error" | "event" | "connected" | "response" | "pong";

  /**
   * Data associated with the response based on the response type.
   */
  data:
    | RealtimeResponseAuthenticated
    | RealtimeResponseConnected
    | RealtimeResponseError
    | RealtimeResponseEvent<unknown>
    | undefined;
};

/**
 * Realtime request structure for authentication.
 */
export type RealtimeRequest = {
  /**
   * Type of the request: 'authentication'.
   */
  type: "authentication";

  /**
   * Data required for authentication.
   */
  data: RealtimeRequestAuthenticate;
};

/**
 * Realtime event response structure with generic payload type.
 */
export type RealtimeResponseEvent<T extends unknown> = {
  /**
   * List of event names associated with the response.
   */
  events: string[];

  /**
   * List of channel names associated with the response.
   */
  channels: string[];

  /**
   * Timestamp indicating the time of the event.
   */
  timestamp: number;

  /**
   * Payload containing event-specific data.
   */
  payload: T;
};

/**
 * Realtime response structure for errors.
 */
export type RealtimeResponseError = {
  /**
   * Numeric error code indicating the type of error.
   */
  code: number;

  /**
   * Error message describing the encountered error.
   */
  message: string;
};

/**
 * Realtime response structure for a successful connection.
 */
export type RealtimeResponseConnected = {
  /**
   * List of channels the user is connected to.
   */
  channels: string[];

  /**
   * User object representing the connected user (optional).
   */
  user?: object;
};

/**
 * Realtime response structure for authenticated connections.
 */
export type RealtimeResponseAuthenticated = {
  /**
   * Destination channel for the response.
   */
  to: string;

  /**
   * Boolean indicating the success of the authentication process.
   */
  success: boolean;

  /**
   * User object representing the authenticated user.
   */
  user: object;
};

/**
 * Realtime request structure for authentication.
 */
export type RealtimeRequestAuthenticate = {
  /**
   * Session identifier for authentication.
   */
  session: string;
};

export type TimeoutHandle = ReturnType<typeof setTimeout> | number;

/**
 * Realtime interface representing the structure of a realtime communication object.
 */
export type Realtime = {
  /**
   * WebSocket instance for realtime communication.
   */
  socket?: Socket;

  /**
   * Timeout for reconnect operations.
   */
  timeout?: TimeoutHandle;

  /**
   * Heartbeat interval for the realtime connection.
   */
  heartbeat?: TimeoutHandle;

  /**
   * URL for establishing the WebSocket connection.
   */
  url?: string;

  /**
   * Last received message from the realtime server.
   */
  lastMessage?: RealtimeResponse;

  /**
   * Set of channel names the client is subscribed to.
   */
  channels: Set<string>;

  /**
   * Map of subscriptions containing channel names and corresponding callback functions.
   */
  subscriptions: Map<
    number,
    {
      channels: string[];
      callback: (payload: RealtimeResponseEvent<any>) => void;
    }
  >;

  /**
   * Counter for managing subscriptions.
   */
  subscriptionsCounter: number;

  /**
   * Boolean indicating whether automatic reconnection is enabled.
   */
  reconnect: boolean;

  /**
   * Number of reconnection attempts made.
   */
  reconnectAttempts: number;

  /**
   * Function to get the timeout duration for communication operations.
   */
  getTimeout: () => number;

  /**
   * Function to establish a WebSocket connection.
   */
  connect: () => void;

  /**
   * Function to create a new WebSocket instance.
   */
  createSocket: () => void;

  /**
   * Function to create a new heartbeat interval.
   */
  createHeartbeat: () => void;

  /**
   * Function to clean up resources associated with specified channels.
   *
   * @param {string[]} channels - List of channel names to clean up.
   */
  cleanUp: (channels: string[]) => void;

  /**
   * Function to handle incoming messages from the WebSocket connection.
   *
   * @param {MessageEvent} event - Event containing the received message.
   */
  onMessage: (event: MessageEvent) => void;
};

/**
 * Type representing upload progress information.
 */
export type UploadProgress = {
  /**
   * Identifier for the upload progress.
   */
  $id: string;

  /**
   * Current progress of the upload (in percentage).
   */
  progress: number;

  /**
   * Total size uploaded (in bytes) during the upload process.
   */
  sizeUploaded: number;

  /**
   * Total number of chunks that need to be uploaded.
   */
  chunksTotal: number;

  /**
   * Number of chunks that have been successfully uploaded.
   */
  chunksUploaded: number;
};

/**
 * BaseClient configuration
 */
export interface Config {
  endpoint: string;
  endpointRealtime: string;
  project: string;
  jwt: string;
  locale: string;
  session: string;
}

interface SuccessResponse<T> {
  data: T;
  error: null;
}

interface ErrorResponse {
  data: null;
  error: NuvixException;
}

export type SafeResponse<T> =
  | (T extends { data: any[]; total: number }
      ? T & { error: null }
      : SuccessResponse<T>)
  | (T extends { data: any[]; total: number }
      ? ErrorResponse & { total: undefined }
      : ErrorResponse);

export type ResponseType<
  T extends BaseClient,
  R,
> = T["safeResponse"] extends true ? SafeResponse<R> : R;
export type PromiseResponseType<T extends BaseClient, R> = Promise<
  T["safeResponse"] extends true ? SafeResponse<R> : R
>;

export interface CollectionSchema {
  __type: "document";
  Types: Record<string, Models.Document>;
}

export interface OtherSchema {
  __type: "managed" | "unmanaged";
  Types: DatabaseTypes.GenericSchema;
}

export type Schemas = Record<string, CollectionSchema | OtherSchema>;

export type GetTableOrView<
  T extends Schemas,
  Schema extends keyof T,
  Table,
> = T[Schema] extends {
  __type: "managed" | "unmanaged";
  Types: infer U extends DatabaseTypes.GenericSchema;
}
  ? Table extends keyof U["Tables"]
    ? U["Tables"][Table]
    : Table extends keyof U["Views"]
      ? U["Views"][Table]
      : never
  : never;

export type GetSchemaType<
  T extends Schemas,
  Schema extends keyof T,
  isDoc extends boolean,
> = isDoc extends true
  ? T[Schema] extends {
      __type: "document";
      Types: infer U extends Record<string, Models.Document>;
    }
    ? U
    : never
  : T[Schema] extends {
        __type: "managed" | "unmanaged";
        Types: infer U extends DatabaseTypes.GenericSchema;
      }
    ? U
    : never;
