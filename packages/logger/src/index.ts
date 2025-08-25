import pino, { type Logger } from "pino";
import chalk from "chalk";

// ============================================================================
// Emoji Mapping
// ============================================================================

export const EMOJI = {
  // Level indicators
  INFO: "ℹ️ ",
  WARN: "⚠️ ",
  ERROR: "❌",
  DEBUG: "🔍",
  SUCCESS: "✅",
  FATAL: "💀",

  // Action types
  START: "🚀",
  STOP: "🛑",
  RESTART: "🔄",
  CONNECT: "🔌",
  DISCONNECT: "🔌",
  SEND: "📤",
  RECEIVE: "📥",
  PUBLISH: "📨",
  CONSUME: "📬",
  WRITE: "💾",
  READ: "📖",
  DELETE: "🗑️",
  UPDATE: "🔄",

  // Status
  OK: "✅",
  PENDING: "⏳",
  PROCESSING: "🔄",
  COMPLETE: "✅",
  FAILED: "❌",

  // Network & Communication
  NETWORK: "🌐",
  API: "🔗",
  WEBSOCKET: "🛰️ ",
  HTTP: "🌍",
  DATABASE: "💾",
  KAFKA: "📦",
  INFLUX: "💾",

  // Data & Analytics
  DATA: "📊",
  METRICS: "📈",
  STATS: "📊",
  CHART: "📉",
  PRICE: "💰",
  COIN: "💎",
  MONEY: "💵",

  // System
  CPU: "🖥️ ",
  MEMORY: "💿",
  DISK: "💿",
  CLOCK: "🕐",
  TIMER: "⏱️ ",
  ROCKET: "🚀",
  FIRE: "🔥",
  LIGHTNING: "⚡",

  // Containers & Structure
  PACKAGE: "📦",
  BOX: "📦",
  FOLDER: "📁",
  FILE: "📄",
  CONFIG: "⚙️ ",

  // Users & Clients
  USER: "👤",
  USERS: "👥",
  CLIENT: "👤",
  CLIENTS: "👥",
  SERVER: "🌐",

  // Special
  STAR: "⭐",
  HEART: "❤️ ",
  PARTY: "🎉",
  CELEBRATE: "🎊",
  TARGET: "🎯",
  BRAIN: "🧠",
  SATELLITE: "🛰️ ",
  BROADCAST: "📡",
} as const;

// ============================================================================
// Log Levels with Colors
// ============================================================================

export type LogLevel = "debug" | "info" | "warn" | "error" | "fatal";

const LEVEL_EMOJI: Record<LogLevel, string> = {
  debug: EMOJI.DEBUG,
  info: EMOJI.INFO,
  warn: EMOJI.WARN,
  error: EMOJI.ERROR,
  fatal: EMOJI.FATAL,
};

const LEVEL_COLOR: Record<LogLevel, (text: string) => string> = {
  debug: chalk.dim,
  info: chalk.blue,
  warn: chalk.yellow,
  error: chalk.red,
  fatal: chalk.bgRed.white.bold,
};

// ============================================================================
// Logger Configuration
// ============================================================================

export interface LoggerOptions {
  name: string;
  level?: LogLevel;
  pretty?: boolean;
  emoji?: boolean;
  timestamp?: boolean;
  forwardToPino?: boolean;
}

// ============================================================================
// Custom Logger Class
// ============================================================================

export class TDATLogger {
  private pino: Logger;
  private appName: string;
  private useEmoji: boolean;
  private useTimestamp: boolean;
  private forwardToPino: boolean;

  constructor(options: LoggerOptions) {
    this.appName = options.name;
    this.useEmoji = options.emoji ?? true;
    this.useTimestamp = options.timestamp ?? true;
    this.forwardToPino = options.forwardToPino ?? false;

    const pinoOptions: any = {
      name: options.name,
      level: options.level || "info",
    };

    if (options.pretty) {
      pinoOptions.transport = {
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "HH:MM:ss.l",
          ignore: "pid,hostname",
          singleLine: false,
        },
      };
    }

    if (!this.forwardToPino) {
      pinoOptions.level = "silent";
    }

    this.pino = pino(pinoOptions);
  }

  private formatMessage(emoji: string, message: string): string {
    const prefix = this.useEmoji ? emoji : "";
    const timestamp = this.useTimestamp ? new Date().toISOString() : "";
    const appPrefix = chalk.cyan(`[${this.appName}]`);

    return `${timestamp} ${appPrefix} ${prefix} ${message}`;
  }

  private formatData(data?: any): string {
    if (!data) return "";
    if (typeof data === "string") return data;
    return JSON.stringify(data, null, 2);
  }

  // ============================================================================
  // Standard Log Levels
  // ============================================================================

  debug(message: string, data?: any): void {
    const formatted = this.formatMessage(LEVEL_EMOJI.debug, message);
    if (data) {
      console.log(LEVEL_COLOR.debug(formatted));
      console.log(LEVEL_COLOR.debug("   └─"), this.formatData(data));
    } else {
      console.log(LEVEL_COLOR.debug(formatted));
    }
    this.pino.debug({ data }, message);
  }

  info(message: string, data?: any): void {
    const formatted = this.formatMessage(LEVEL_EMOJI.info, message);
    if (data) {
      console.log(LEVEL_COLOR.info(formatted));
      console.log(LEVEL_COLOR.info("   └─"), this.formatData(data));
    } else {
      console.log(LEVEL_COLOR.info(formatted));
    }
    this.pino.info({ data }, message);
  }

  warn(message: string, data?: any): void {
    const formatted = this.formatMessage(LEVEL_EMOJI.warn, message);
    if (data) {
      console.log(LEVEL_COLOR.warn(formatted));
      console.log(LEVEL_COLOR.warn("   └─"), this.formatData(data));
    } else {
      console.log(LEVEL_COLOR.warn(formatted));
    }
    this.pino.warn({ data }, message);
  }

  error(message: string, data?: any): void {
    const formatted = this.formatMessage(LEVEL_EMOJI.error, message);
    if (data) {
      console.log(LEVEL_COLOR.error(formatted));
      console.log(LEVEL_COLOR.error("   └─"), this.formatData(data));
    } else {
      console.log(LEVEL_COLOR.error(formatted));
    }
    this.pino.error({ data }, message);
  }

  fatal(message: string, data?: any): void {
    const formatted = this.formatMessage(LEVEL_EMOJI.fatal, message);
    if (data) {
      console.log(LEVEL_COLOR.fatal(formatted));
      console.log(LEVEL_COLOR.fatal("   └─"), this.formatData(data));
    } else {
      console.log(LEVEL_COLOR.fatal(formatted));
    }
    this.pino.fatal({ data }, message);
  }

  // ============================================================================
  // Semantic Logging Methods
  // ============================================================================

  success(message: string, data?: any): void {
    const formatted = this.formatMessage(EMOJI.SUCCESS, message);
    console.log(chalk.green(formatted));
    if (data) {
      console.log(chalk.green("   └─"), this.formatData(data));
    }
    this.pino.info({ data, type: "success" }, message);
  }

  start(message: string, data?: any): void {
    const formatted = this.formatMessage(EMOJI.START, message);
    console.log(chalk.cyan.bold(formatted));
    if (data) {
      console.log(chalk.cyan("   └─"), this.formatData(data));
    }
    this.pino.info({ data, type: "start" }, message);
  }

  stop(message: string, data?: any): void {
    const formatted = this.formatMessage(EMOJI.STOP, message);
    console.log(chalk.red(formatted));
    if (data) {
      console.log(chalk.red("   └─"), this.formatData(data));
    }
    this.pino.info({ data, type: "stop" }, message);
  }

  kafka(
    action: "send" | "receive" | "connect",
    message: string,
    data?: any
  ): void {
    const emoji =
      action === "send"
        ? EMOJI.SEND
        : action === "receive"
        ? EMOJI.RECEIVE
        : EMOJI.CONNECT;
    const formatted = this.formatMessage(emoji, `[KAFKA] ${message}`);
    console.log(chalk.magenta(formatted));
    if (data) {
      console.log(chalk.magenta("   └─"), this.formatData(data));
    }
    this.pino.info({ data, type: "kafka", action }, message);
  }

  influx(
    action: "write" | "read" | "query",
    message: string,
    data?: any
  ): void {
    const emoji =
      action === "write"
        ? EMOJI.WRITE
        : action === "read"
        ? EMOJI.READ
        : EMOJI.DATABASE;
    const formatted = this.formatMessage(emoji, `[INFLUXDB] ${message}`);
    console.log(chalk.blue(formatted));
    if (data) {
      console.log(chalk.blue("   └─"), this.formatData(data));
    }
    this.pino.info({ data, type: "influx", action }, message);
  }

  http(method: string, path: string, status: number, duration?: number): void {
    const statusColor =
      status >= 500 ? chalk.red : status >= 400 ? chalk.yellow : chalk.green;
    const formatted = this.formatMessage(
      EMOJI.HTTP,
      `${method.toUpperCase()} ${path} ${statusColor(status.toString())} ${
        duration ? `(${duration}ms)` : ""
      }`
    );
    console.log(formatted);
    this.pino.info({ method, path, status, duration }, "HTTP Request");
  }

  ws(
    action: "connect" | "disconnect" | "message",
    message: string,
    data?: any
  ): void {
    const emoji =
      action === "connect"
        ? EMOJI.CONNECT
        : action === "disconnect"
        ? EMOJI.DISCONNECT
        : EMOJI.WEBSOCKET;
    const formatted = this.formatMessage(emoji, `[WEBSOCKET] ${message}`);
    console.log(chalk.cyan(formatted));
    if (data) {
      console.log(chalk.cyan("   └─"), this.formatData(data));
    }
    this.pino.info({ data, type: "websocket", action }, message);
  }

  price(coin: string, price: number, currency: string = "USD"): void {
    const formatted = this.formatMessage(
      EMOJI.PRICE,
      `${chalk.yellow(coin)} → ${chalk.green(
        "$" + price.toFixed(2)
      )} ${currency}`
    );
    console.log(formatted);
    this.pino.info({ coin, price, currency }, "Price Update");
  }

  metric(name: string, value: number | string, unit?: string): void {
    const formatted = this.formatMessage(
      EMOJI.METRICS,
      `${name}: ${chalk.cyan(value.toString())} ${unit || ""}`
    );
    console.log(formatted);
    this.pino.info({ metric: name, value, unit }, "Metric");
  }

  // ============================================================================
  // Box/Banner Logging
  // ============================================================================

  box(title: string, lines: string[]): void {
    const maxLength = Math.max(title.length, ...lines.map((l) => l.length)) + 4;
    const top = chalk.cyan("╔" + "═".repeat(maxLength) + "╗");
    const titleLine = chalk.cyan(
      `║ ${chalk.white.bold(title.padEnd(maxLength - 2))}║`
    );
    const separator = chalk.cyan("╠" + "═".repeat(maxLength) + "╣");
    const bottom = chalk.cyan("╚" + "═".repeat(maxLength) + "╝");

    console.log("\n" + top);
    console.log(titleLine);
    if (lines.length > 0) {
      console.log(separator);
      lines.forEach((line) => {
        console.log(chalk.cyan(`║ ${line.padEnd(maxLength - 2)}║`));
      });
    }
    console.log(bottom + "\n");
  }

  separator(char: string = "─", length: number = 80): void {
    console.log(chalk.dim(char.repeat(length)));
  }

  // ============================================================================
  // Progress & Stats
  // ============================================================================

  progress(current: number, total: number, label?: string): void {
    const percent = Math.round((current / total) * 100);
    const bar =
      "█".repeat(Math.floor(percent / 5)) +
      "░".repeat(20 - Math.floor(percent / 5));
    const formatted = this.formatMessage(
      EMOJI.PROCESSING,
      `${label ? label + ": " : ""}[${chalk.cyan(
        bar
      )}] ${percent}% (${current}/${total})`
    );
    process.stdout.write("\r" + formatted);
    if (current === total) {
      console.log(); // New line when complete
    }
  }

  stats(data: Record<string, number | string>): void {
    this.box(
      "📊 STATISTICS",
      Object.entries(data).map(
        ([key, value]) => `${key.padEnd(20)}: ${chalk.cyan(value.toString())}`
      )
    );
  }
}

// ============================================================================
// Factory Function
// ============================================================================

export function createLogger(options: LoggerOptions): TDATLogger {
  return new TDATLogger(options);
}

// ============================================================================
// Default Export
// ============================================================================

export default createLogger;
