import { PromiseResponseType } from 'types';
import { Client, type Payload } from '../client';
import type { Models } from '../models';

export class Locale<T extends Client = Client> {
    client: T;

    constructor(client: T) {
        this.client = client;
    }

    /**
     * Get user locale
     *
     * Get the current user location based on IP. Returns an object with user country code, country name, continent name, continent code, ip address and suggested currency. You can use the locale header to get the data in a supported language. ([IP Geolocation by DB-IP](https://db-ip.com))
     *
     * @returns {PromiseResponseType<T, Models.Locale>}
     */
    async get(): PromiseResponseType<T, Models.Locale> {
        const apiPath = '/locale';
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
    /**
     * List locale codes
     *
     * List of all locale codes in [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes).
     *
     * @returns {PromiseResponseType<T, Models.LocaleCodeList>}
     */
    async listCodes(): PromiseResponseType<T, Models.LocaleCodeList> {
        const apiPath = '/locale/codes';
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
    /**
     * List continents
     *
     * List of all continents. You can use the locale header to get the data in a supported language.
     *
     * @returns {PromiseResponseType<T, Models.ContinentList>}
     */
    async listContinents(): PromiseResponseType<T, Models.ContinentList> {
        const apiPath = '/locale/continents';
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
    /**
     * List countries
     *
     * List of all countries. You can use the locale header to get the data in a supported language.
     *
     * @returns {PromiseResponseType<T, Models.CountryList>}
     */
    async listCountries(): PromiseResponseType<T, Models.CountryList> {
        const apiPath = '/locale/countries';
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
    /**
     * List EU countries
     *
     * List of all countries that are currently members of the EU. You can use the locale header to get the data in a supported language.
     *
     * @returns {PromiseResponseType<T, Models.CountryList>}
     */
    async listCountriesEU(): PromiseResponseType<T, Models.CountryList> {
        const apiPath = '/locale/countries/eu';
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
    /**
     * List countries phone codes
     *
     * List of all countries phone codes. You can use the locale header to get the data in a supported language.
     *
     * @returns {PromiseResponseType<T, Models.PhoneList>}
     */
    async listCountriesPhones(): PromiseResponseType<T, Models.PhoneList> {
        const apiPath = '/locale/countries/phones';
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
    /**
     * List currencies
     *
     * List of all currencies, including currency symbol, name, plural, and decimal digits for all major and minor currencies. You can use the locale header to get the data in a supported language.
     *
     * @returns {PromiseResponseType<T, Models.CurrencyList>}
     */
    async listCurrencies(): PromiseResponseType<T, Models.CurrencyList> {
        const apiPath = '/locale/currencies';
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
    /**
     * List languages
     *
     * List of all languages classified by ISO 639-1 including 2-letter code, name in English, and name in the respective language.
     *
     * @returns {PromiseResponseType<T, Models.LanguageList>}
     */
    async listLanguages(): PromiseResponseType<T, Models.LanguageList> {
        const apiPath = '/locale/languages';
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
