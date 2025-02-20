import { Client, Locale } from "nuvix";

const client = new Client()
    .setEndpoint('https://cloud.nuvix.io/v1') // Your API Endpoint
    .setProject('<YOUR_PROJECT_ID>'); // Your project ID

const locale = new Locale(client);

const result = await locale.listCountriesPhones();

console.log(result);
