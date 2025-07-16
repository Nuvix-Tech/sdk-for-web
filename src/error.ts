/**
 * Exception thrown by the  package
 */
export class NuvixException extends Error {
  /**
   * The error code associated with the exception.
   */
  code: number;

  /**
   * The response string associated with the exception.
   */
  response: string;

  /**
   * Error type.
   * See [Error Types](https://nuvix.io/docs/response-codes#errorTypes) for more information.
   */
  type: string;

  /**
   * Initializes a Nuvix Exception.
   *
   * @param {string} message - The error message.
   * @param {number} code - The error code. Default is 0.
   * @param {string} type - The error type. Default is an empty string.
   * @param {string} response - The response string. Default is an empty string.
   */
  constructor(
    message: string,
    code: number = 0,
    type: string = "",
    response: string = "",
  ) {
    super(message);
    this.name = "NuvixException";
    this.message = message;
    this.code = code;
    this.type = type;
    this.response = response;
  }
}
