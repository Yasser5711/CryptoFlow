# @cryptoflow/banner

Shared banner utility for CryptoFlow applications.

## Installation

```bash
npm install @cryptoflow/banner
```

## Usage

```typescript
import { printBanner } from "@cryptoflow/banner";

printBanner("MY-APP", "Server listening at http://0.0.0.0:3000");
```

This will display:

```
 ___  ___  _  _           _     ___  ___
|  \/  | || || |         /_\   | _ \| _ \
| |\/| |\_  _/ |        / _ \  |  _/|  _/
|_|  |_|  ||_|         /_/ \_\ |_|  |_|

███████████████████████████████████████████████████████
 Server listening at http://0.0.0.0:3000
███████████████████████████████████████████████████████
```

## API

### `printBanner(appName: string, status: string): void`

Displays an ASCII art banner with a status line.

- **appName**: The name of the application (will be displayed in ASCII art)
- **status**: A status message to display below the banner

## License

MIT
