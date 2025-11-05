import { Client, AuthenticatorType } from "nuvix";

const nx = new Client()
.setEndpoint('https://api.nuvix.in/v1') // Your API Endpoint
.setProject('<YOUR_PROJECT_ID>'); // Your project ID

const result = await nx.account.deleteMfaAuthenticator(
AuthenticatorType.Totp // type
);

console.log(result);
