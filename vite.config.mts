import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig(({ command }) => ({
  root: fileURLToPath(
    new URL("./src/DotCelery.Dashboard/Frontend", import.meta.url),
  ),
  base: "./",
  plugins: [
    react(),
    tailwindcss(),
    {
      name: "dashboard-development-shell",
      transformIndexHtml(html) {
        return command === "serve"
          ? html
              .replaceAll("{{PREFIX}}", "/celery")
              .replaceAll("{{TITLE}}", "DotCelery Dashboard (development)")
          : html;
      },
    },
  ],
  build: {
    outDir: "../obj/dashboard",
    emptyOutDir: true,
    // Matches Tailwind 4's browser floor. Existing AbortSignal APIs still require
    // current browsers, as they did in the JavaScript dashboard.
    target: ["chrome111", "safari16.4", "firefox128"],
  },
  server: {
    proxy: {
      "/celery/api": {
        target:
          process.env.DOTCELERY_DASHBOARD_BACKEND ?? "http://localhost:5099",
        // Keep the browser's Host so the backend's same-origin check still applies.
        changeOrigin: false,
      },
      "/celery/hub": {
        target:
          process.env.DOTCELERY_DASHBOARD_BACKEND ?? "http://localhost:5099",
        changeOrigin: false,
        ws: true,
      },
    },
  },
}));
