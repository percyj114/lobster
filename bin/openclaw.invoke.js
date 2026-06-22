#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

function shellQuote(arg) {
	// Conservative POSIX-ish quoting for embedding argv into a single pipeline string.
	// Lobster's pipeline parser preserves quoted substrings.
	if (/^[A-Za-z0-9_\-./:=@]+$/.test(arg)) return arg;
	// single-quote, escaping embedded single quotes: ' -> '\''
	return `'${String(arg).replace(/'/g, `'\\''`)}'`;
}

const argv = process.argv.slice(2);
const pipeline = ["openclaw.invoke", ...argv.map(shellQuote)].join(" ");
const lobsterBin = join(dirname(fileURLToPath(import.meta.url)), "lobster.js");

const res = spawnSync(process.execPath, [lobsterBin, pipeline], {
	stdio: "inherit",
	env: process.env,
});
if (res.error) {
	console.error(`openclaw.invoke failed to spawn lobster: ${res.error.message}`);
}

process.exit(res.status ?? 1);
