import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";

test("packaged lobster bin starts and prints help", () => {
	const bin = path.join(process.cwd(), "bin", "lobster.js");
	const res = spawnSync(process.execPath, [bin, "--help"], {
		encoding: "utf8",
	});

	assert.equal(res.status, 0, res.stderr);
	assert.match(res.stdout, /Usage:/);
});
