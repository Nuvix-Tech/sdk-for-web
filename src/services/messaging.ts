import { NuvixException, Client, type Payload } from '../client';
import type { Models } from '../models';
import { PromiseResponseType } from 'types';

export class Messaging<T extends Client> {
    client: T;

    constructor(client: T) {
        this.client = client;
    }

    /**
     * Create subscriber
     *
     * Create a new subscriber.
     *
     * @param {string} topicId
     * @param {string} subscriberId
     * @param {string} targetId
     * @returns {PromiseResponseType<T, Models.Subscriber>}
     */
    async createSubscriber(topicId: string, subscriberId: string, targetId: string): PromiseResponseType<T, Models.Subscriber> {
        if (typeof topicId === 'undefined') {
            throw new NuvixException('Missing required parameter: "topicId"');
        }
        if (typeof subscriberId === 'undefined') {
            throw new NuvixException('Missing required parameter: "subscriberId"');
        }
        if (typeof targetId === 'undefined') {
            throw new NuvixException('Missing required parameter: "targetId"');
        }
        const apiPath = '/messaging/topics/{topicId}/subscribers'.replace('{topicId}', topicId);
        const payload: Payload = {};
        if (typeof subscriberId !== 'undefined') {
            payload['subscriberId'] = subscriberId;
        }
        if (typeof targetId !== 'undefined') {
            payload['targetId'] = targetId;
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
     * Delete subscriber
     *
     * Delete a subscriber by its unique ID.
     *
     * @param {string} topicId
     * @param {string} subscriberId
     * @returns {PromiseResponseType<T, {}>}
     */
    async deleteSubscriber(topicId: string, subscriberId: string): PromiseResponseType<T, {}> {
        if (typeof topicId === 'undefined') {
            throw new NuvixException('Missing required parameter: "topicId"');
        }
        if (typeof subscriberId === 'undefined') {
            throw new NuvixException('Missing required parameter: "subscriberId"');
        }
        const apiPath = '/messaging/topics/{topicId}/subscribers/{subscriberId}'.replace('{topicId}', topicId).replace('{subscriberId}', subscriberId);
        const payload: Payload = {};
        const uri = new URL(this.client.config.endpoint + apiPath);

        const apiHeaders: { [header: string]: string } = {
            'content-type': 'application/json',
        }


        return await this.client.call(
            'delete',
            uri,
            apiHeaders,
            payload
        );
    }
}
