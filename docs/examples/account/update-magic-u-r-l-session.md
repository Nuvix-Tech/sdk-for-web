import { Client, Account } from "nuvix";

const client = new Client()
    .setEndpoint('https://cloud.nuvix.io/v1') // Your API Endpoint
    .setProject('<YOUR_PROJECT_ID>'); // Your project ID

const account = new Account(client);

const result = await account.updateMagicURLSession(
    '<USER_ID>', // userId
    '<SECRET>' // secret
);

console.log(result);
