import { Service } from '../service';
import { NuvixException, Client, type Payload, UploadProgress } from '../client';
import type { Models } from '../models';
import { ExecutionMethod } from '../enums/execution-method';

export class Functions {
    client: Client;

    constructor(client: Client) {
        this.client = client;
    }

    /**
     * List executions
     *
     * Get a list of all the current user function execution logs. You can use the query params to filter your results.
     *
     * @param {string} functionId
     * @param {string[]} queries
     * @param {string} search
     * @throws {NuvixException}
     * @returns {Promise<Models.ExecutionList>}
     */
    async listExecutions(functionId: string, queries?: string[], search?: string): Promise<Models.ExecutionList> {
        if (typeof functionId === 'undefined') {
            throw new NuvixException('Missing required parameter: "functionId"');
        }
        const apiPath = '/functions/{functionId}/executions'.replace('{functionId}', functionId);
        const payload: Payload = {};
        if (typeof queries !== 'undefined') {
            payload['queries'] = queries;
        }
        if (typeof search !== 'undefined') {
            payload['search'] = search;
        }
        const uri = new URL(this.client.config.endpoint + apiPath);

        const apiHeaders: { [header: string]: string } = {
            'content-type': 'application/json',
        }


        return await this.client.call(
            'get',
            uri,
            apiHeaders,
            payload
        );
    }
    /**
     * Create execution
     *
     * Trigger a function execution. The returned object will return you the current execution status. You can ping the `Get Execution` endpoint to get updates on the current execution status. Once this endpoint is called, your function execution process will start asynchronously.
     *
     * @param {string} functionId
     * @param {string} body
     * @param {boolean} async
     * @param {string} xpath
     * @param {ExecutionMethod} method
     * @param {object} headers
     * @param {string} scheduledAt
     * @throws {NuvixException}
     * @returns {Promise<Models.Execution>}
     */
    async createExecution(functionId: string, body?: string, async?: boolean, xpath?: string, method?: ExecutionMethod, headers?: object, scheduledAt?: string): Promise<Models.Execution> {
        if (typeof functionId === 'undefined') {
            throw new NuvixException('Missing required parameter: "functionId"');
        }
        const apiPath = '/functions/{functionId}/executions'.replace('{functionId}', functionId);
        const payload: Payload = {};
        if (typeof body !== 'undefined') {
            payload['body'] = body;
        }
        if (typeof async !== 'undefined') {
            payload['async'] = async;
        }
        if (typeof xpath !== 'undefined') {
            payload['path'] = xpath;
        }
        if (typeof method !== 'undefined') {
            payload['method'] = method;
        }
        if (typeof headers !== 'undefined') {
            payload['headers'] = headers;
        }
        if (typeof scheduledAt !== 'undefined') {
            payload['scheduledAt'] = scheduledAt;
        }
        const uri = new URL(this.client.config.endpoint + apiPath);

        const apiHeaders: { [header: string]: string } = {
            'content-type': 'application/json',
        }


        return await this.client.call(
            'post',
            uri,
            apiHeaders,
            payload
        );
    }
    /**
     * Get execution
     *
     * Get a function execution log by its unique ID.
     *
     * @param {string} functionId
     * @param {string} executionId
     * @throws {NuvixException}
     * @returns {Promise<Models.Execution>}
     */
    async getExecution(functionId: string, executionId: string): Promise<Models.Execution> {
        if (typeof functionId === 'undefined') {
            throw new NuvixException('Missing required parameter: "functionId"');
        }
        if (typeof executionId === 'undefined') {
            throw new NuvixException('Missing required parameter: "executionId"');
        }
        const apiPath = '/functions/{functionId}/executions/{executionId}'.replace('{functionId}', functionId).replace('{executionId}', executionId);
        const payload: Payload = {};
        const uri = new URL(this.client.config.endpoint + apiPath);

        const apiHeaders: { [header: string]: string } = {
            'content-type': 'application/json',
        }


        return await this.client.call(
            'get',
            uri,
            apiHeaders,
            payload
        );
    }
}
