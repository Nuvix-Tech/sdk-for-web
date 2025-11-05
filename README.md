# Nuvix Web SDK

![License](https://img.shields.io/github/license/nuvix-tech/sdk-for-web.svg?style=flat-square)
![Version](https://img.shields.io/npm/v/@nuvix/client.svg?style=flat-square)

**This SDK is compatible with Nuvix server version 1.0.x. For older versions, please check [previous releases](https://github.com/nuvix-tech/sdk-for-web/releases).**

Nuvix is an open-source high-performance backend platform built for modern applications—fast, scalable, and developer-first. Use the Web SDK to integrate your app with the Nuvix server to easily start interacting with all of Nuvix backend APIs and tools. For full API documentation and tutorials go to [https://www.nuvix.in](https://www.nuvix.in)

## Installation

### NPM

To install via [NPM](https://www.npmjs.com/):

```bash
npm install @nuvix/client
```

If you're using a bundler (like [Rollup](https://rollupjs.org/) or [webpack](https://webpack.js.org/)), you can import the Nuvix module when you need it:

```js
import { Client, ID } from "@nuvix/client";
```

### CDN

To install with a CDN (content delivery network) add the following scripts to the bottom of your <body> tag, but before you use any Nuvix services:

```html
<script src="https://cdn.jsdelivr.net/npm/@nuvix/client"></script>
```

## Getting Started

### Add your Web Platform

For you to init your SDK and interact with Nuvix services you need to add a web platform to your project. To add a new platform, go to your Nuvix console, choose the project you created in the step before and click the 'Add Platform' button.

From the options, choose to add a **Web** platform and add your client app hostname. By adding your hostname to your project platform you are allowing cross-domain communication between your project and the Nuvix API.

### Init your SDK

Initialize your SDK with your Nuvix server API endpoint and project ID which can be found in your project settings page.

```js
import { Client } from "@nuvix/client";

// Init your Web SDK
const client = new Client();

client
  .setEndpoint("https://api.nuvix.in/v1") // Your Nuvix Endpoint
  .setProject("your-project-id"); // Your project ID
```

### Make Your First Request

Once your SDK object is set, access any of the Nuvix services and choose any request to send. Full documentation for any service method you would like to use can be found in your SDK documentation or in the API References section.

```js
import { Client, ID } from "@nuvix/client";

const client = new Client();

client.setEndpoint("https://api.nuvix.in/v1").setProject("your-project-id");

// Register User
client.account
  .create(ID.unique(), "email@example.com", "password", "Walter O'Brien")
  .then(
    function (response) {
      console.log(response);
    },
    function (error) {
      console.log(error);
    },
  );
```

### Full Example

```js
import { Client, ID } from "@nuvix/client";

// Init your Web SDK
const client = new Client();

client
  .setEndpoint("https://api.nuvix.in/v1") // Your Nuvix Endpoint
  .setProject("your-project-id"); // Your project ID

// Register User
client.account
  .create(ID.unique(), "email@example.com", "password", "Walter O'Brien")
  .then(
    function (response) {
      console.log(response);
    },
    function (error) {
      console.log(error);
    },
  );

// Using async/await
async function createUser() {
  try {
    const user = await client.account.create(
      ID.unique(),
      "email@example.com",
      "password",
      "Walter O'Brien",
    );
    console.log(user);
  } catch (error) {
    console.error(error);
  }
}
```

### Available Services

The Nuvix Client provides access to the following services:

- **Account** - User authentication and account management
- **Database** - Database operations with type-safe queries
- **Storage** - File storage and management
- **Teams** - Team collaboration features
- **Messaging** - Messaging capabilities
- **Avatars** - Avatar generation
- **Locale** - Localization services

### Database Example

```js
import { Client } from "@nuvix/client";

const client = new Client();

client.setEndpoint("https://api.nuvix.in/v1").setProject("your-project-id");

// Query database
const users = await client.database.from("users").select("id", "name", "email");

console.log(users);
```

### Type-Safe Client with Safe Response Mode

```typescript
import { Client } from "@nuvix/client";

// Enable safe response mode for error handling
const client = new Client({ safeResponse: true });

client.setEndpoint("https://api.nuvix.in/v1").setProject("your-project-id");

// With safe response, methods return { error: Error, data?: T }
const result = await client.account.get();

if (!result.error) {
  console.log(result.data);
} else {
  console.error(result.error);
}
```

## Learn more

You can use the following resources to learn more and get help:

- 🌐 [Nuvix Website](https://www.nuvix.in)
- � [Support](https://www.nuvix.in/support)
- � [NPM Package](https://www.npmjs.com/package/@nuvix/client)
- 💻 [GitHub Repository](https://github.com/Nuvix-Tech/sdk-for-web)

## Contribution

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Please see the [BSD-3-Clause license](https://raw.githubusercontent.com/nuvix-tech/nuvix/main/LICENSE) file for more information.
