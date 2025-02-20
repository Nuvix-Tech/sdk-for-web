import { Client, Avatars } from "nuvix";

const client = new Client()
    .setEndpoint('https://cloud.nuvix.io/v1') // Your API Endpoint
    .setProject('<YOUR_PROJECT_ID>'); // Your project ID

const avatars = new Avatars(client);

const result = avatars.getInitials(
    '<NAME>', // name (optional)
    0, // width (optional)
    0, // height (optional)
    '' // background (optional)
);

console.log(result);
