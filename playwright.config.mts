import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./eng",
  testMatch: "dashboard.browser.mts",
  fullyParallel: false,
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:5099",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command:
      "dotnet run --project samples/DotCelery.Dashboard.Demo --configuration Release --no-launch-profile -- --urls http://127.0.0.1:5099 --Dashboard:PathPrefix=/jobs/monitor",
    url: "http://127.0.0.1:5099/jobs/monitor/",
    timeout: 120000,
    reuseExistingServer: false,
  },
});
