import { Client, Storage } from "nuvix";

const client = new Client()
.setEndpoint('https://cloud.nuvix.io/v1') // Your API Endpoint
.setProject('<YOUR_PROJECT_ID>'); // Your project ID

const storage = new Storage(client);

const result = await storage.updateFile(
'<BUCKET_ID>', // bucketId
'<FILE_ID>', // fileId
'<NAME>', // name (optional)
["read("any")"] // permissions (optional)
);

console.log(result);
