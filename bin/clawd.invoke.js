#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

function shellQuote(arg) {
	if (/^[A-Za-z0-9_\-./:=@]+$/.test(arg)) return arg;
	return `'${String(arg).replace(/'/g, `'\\''`)}'`;
}

const argv = process.argv.slice(2);
const pipeline = ["clawd.invoke", ...argv.map(shellQuote)].join(" ");
const lobsterBin = join(dirname(fileURLToPath(import.meta.url)), "lobster.js");

const res = spawnSync(process.execPath, [lobsterBin, pipeline], {
	stdio: "inherit",
	env: process.env,
});
if (res.error) {
	console.error(`clawd.invoke failed to spawn lobster: ${res.error.message}`);
}

process.exit(res.status ?? 1);
