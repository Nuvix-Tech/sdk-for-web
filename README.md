# Nuvix Web SDK

![License](https://img.shields.io/github/license/nuvix-tech/sdk-for-web.svg?style=flat-square)
![Version](https://img.shields.io/badge/api%20version-1.0.0-blue.svg?style=flat-square)

**This SDK is compatible with Nuvix server version 1.0.x. For older versions, please check [previous releases](https://github.com/nuvix-tech/sdk-for-web/releases).**

Nuvix is an open-source backend as a service server that abstract and simplify complex and repetitive development tasks behind a very simple to use REST API. Nuvix aims to help you develop your apps faster and in a more secure way. Use the Web SDK to integrate your app with the Nuvix server to easily start interacting with all of Nuvix backend APIs and tools. For full API documentation and tutorials go to [https://nuvix.io/docs](https://nuvix.io/docs)

## Installation

### NPM

To install via [NPM](https://www.npmjs.com/):

```bash
npm install nuvix --save
```

If you're using a bundler (like [Rollup](https://rollupjs.org/) or [webpack](https://webpack.js.org/)), you can import the Nuvix module when you need it:

```js
import { Client, Account } from "nuvix";
```

### CDN

To install with a CDN (content delivery network) add the following scripts to the bottom of your <body> tag, but before you use any Nuvix services:

```html
<script src="https://cdn.jsdelivr.net/npm/nuvix@1.0.0"></script>
```


## Getting Started

### Add your Web Platform
For you to init your SDK and interact with Nuvix services you need to add a web platform to your project. To add a new platform, go to your Nuvix console, choose the project you created in the step before and click the 'Add Platform' button.

From the options, choose to add a **Web** platform and add your client app hostname. By adding your hostname to your project platform you are allowing cross-domain communication between your project and the Nuvix API.

### Init your SDK
Initialize your SDK with your Nuvix server API endpoint and project ID which can be found in your project settings page.

```js
// Init your Web SDK
const client = new Client();

client
    .setEndpoint('http://localhost/v1') // Your Nuvix Endpoint
    .setProject('455x34dfkj') // Your project ID
;
```

### Make Your First Request
Once your SDK object is set, access any of the Nuvix services and choose any request to send. Full documentation for any service method you would like to use can be found in your SDK documentation or in the [API References](https://nuvix.io/docs) section.

```js
const account = new Account(client);

// Register User
account.create(ID.unique(), "email@example.com", "password", "Walter O'Brien")
    .then(function (response) {
        console.log(response);
    }, function (error) {
        console.log(error);
    });

```

### Full Example
```js
// Init your Web SDK
const client = new Client();

client
    .setEndpoint('http://localhost/v1') // Your Nuvix Endpoint
    .setProject('455x34dfkj')
;

const account = new Account(client);

// Register User
account.create(ID.unique(), "email@example.com", "password", "Walter O'Brien")
    .then(function (response) {
        console.log(response);
    }, function (error) {
        console.log(error);
    });
```

### Learn more
You can use the following resources to learn more and get help
- 🚀 [Getting Started Tutorial](https://nuvix.io/docs/getting-started-for-web)
- 📜 [Nuvix Docs](https://nuvix.io/docs)
- 💬 [Discord Community](https://nuvix.io/discord)
- 🚂 [Nuvix Web Playground](https://github.com/nuvix-tech/playground-for-web)


## Contribution
## License

Please see the [BSD-3-Clause license](https://raw.githubusercontent.com/nuvix-tech/nuvix/main/LICENSE) file for more information.