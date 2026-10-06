import { spawn } from "node:child_process";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)));
const projects = ["re1", "re2", "re3", "re4", "re5"];
const children = [];

function clearProjectPorts() {
  if (process.platform !== "win32") return;
  const command = "$ports = 5173,5174,5175,5176,5177; $pids = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort } | Select-Object -ExpandProperty OwningProcess -Unique; foreach ($processId in $pids) { Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue }";
  execFileSync("powershell.exe", ["-NoProfile", "-Command", command], { stdio: "ignore" });
}

clearProjectPorts();

for (const project of projects) {
  const directory = resolve(root, project);
  const command = process.platform === "win32" ? "cmd.exe" : "npm";
  const args = process.platform === "win32" ? ["/d", "/s", "/c", "npm.cmd run dev"] : ["run", "dev"];
  const child = spawn(command, args, {
    cwd: directory,
    stdio: "pipe",
    env: process.env,
  });

  child.stdout.on("data", (data) => process.stdout.write(`[${project}] ${data}`));
  child.stderr.on("data", (data) => process.stderr.write(`[${project}] ${data}`));
  child.on("error", (error) => console.error(`[${project}] ${error.message}`));
  children.push(child);
}

console.log("Started RE1, RE2, RE3, RE4, and RE5.");
console.log("Open RE1 at http://127.0.0.1:5173/");
console.log("Press Ctrl+C to stop all frontend servers.");

function stopAll() {
  for (const child of children) {
    if (process.platform === "win32") {
      spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
    } else {
      child.kill("SIGTERM");
    }
  }
}

process.on("SIGINT", () => {
  stopAll();
  process.exit(0);
});
process.on("SIGTERM", () => {
  stopAll();
  process.exit(0);
});
