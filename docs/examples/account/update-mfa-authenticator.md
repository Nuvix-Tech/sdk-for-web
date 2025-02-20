import { Client, Account, AuthenticatorType } from "nuvix";

const client = new Client()
    .setEndpoint('https://cloud.nuvix.io/v1') // Your API Endpoint
    .setProject('<YOUR_PROJECT_ID>'); // Your project ID

const account = new Account(client);

const result = await account.updateMfaAuthenticator(
    AuthenticatorType.Totp, // type
    '<OTP>' // otp
);

console.log(result);
