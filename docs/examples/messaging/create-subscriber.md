import { Client, Messaging } from "nuvix";

const client = new Client()
.setEndpoint('https://cloud.nuvix.io/v1') // Your API Endpoint
.setProject('<YOUR_PROJECT_ID>'); // Your project ID

const messaging = new Messaging(client);

const result = await messaging.createSubscriber(
'<TOPIC_ID>', // topicId
'<SUBSCRIBER_ID>', // subscriberId
'<TARGET_ID>' // targetId
);

console.log(result);
