import { Client, Teams } from "nuvix";

const client = new Client()
.setEndpoint('https://cloud.nuvix.io/v1') // Your API Endpoint
.setProject('<YOUR_PROJECT_ID>'); // Your project ID

const teams = new Teams(client);

const result = await teams.list(
[], // queries (optional)
'<SEARCH>' // search (optional)
);

console.log(result);
