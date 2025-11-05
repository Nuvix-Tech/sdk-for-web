import { Client, CreditCard } from "nuvix";

const nx = new Client()
.setEndpoint('https://api.nuvix.in/v1') // Your API Endpoint
.setProject('<YOUR_PROJECT_ID>'); // Your project ID

const result = nx.avatars.getCreditCard(
CreditCard.AmericanExpress, // code
0, // width (optional)
0, // height (optional)
0 // quality (optional)
);

console.log(result);
