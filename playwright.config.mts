import { defineConfig, devices } from "@playwright/test";

const framework = process.env.DOTCELERY_TEST_FRAMEWORK ?? "net11.0";
if (framework !== "net10.0" && framework !== "net11.0") {
  throw new Error("DOTCELERY_TEST_FRAMEWORK must be net10.0 or net11.0.");
}

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
    command: `dotnet run --project samples/DotCelery.Dashboard.Demo --framework ${framework} --configuration Release --no-launch-profile -- --urls http://127.0.0.1:5099 --Dashboard:PathPrefix=/jobs/monitor`,
    url: "http://127.0.0.1:5099/jobs/monitor/",
    timeout: 120000,
    reuseExistingServer: false,
  },
});
