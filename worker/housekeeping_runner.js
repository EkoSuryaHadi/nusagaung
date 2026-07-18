const { exec } = require("child_process");
const path = require("path");

const INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 hours

function runHousekeeping() {
  console.log(`[${new Date().toISOString()}] Running Database Housekeeping Maintenance...`);
  const scriptPath = path.join(__dirname, "housekeeping.py");
  const command = `python "${scriptPath}" --apply`;

  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`[Housekeeping Error] Execution failed:`, error.message);
      return;
    }
    if (stderr) {
      console.warn(`[Housekeeping Warning]:`, stderr);
    }
    console.log(`[Housekeeping Output]:\n${stdout}`);
  });
}

// Initial run
runHousekeeping();

// Schedule daily run
setInterval(runHousekeeping, INTERVAL_MS);
console.log("[Housekeeping Runner] Daily maintenance scheduler started.");
