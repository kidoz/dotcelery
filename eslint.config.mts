import js from "@eslint/js";
import { defineConfig, globalIgnores } from "eslint/config";
import prettier from "eslint-config-prettier/flat";
import globals from "globals";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";

export default defineConfig(
  globalIgnores([
    "**/node_modules/**",
    "**/bin/**",
    "**/obj/**",
    "**/dist/**",
    "artifacts/**",
  ]),
  {
    files: ["src/DotCelery.Dashboard/wwwroot/**/*.js"],
    extends: [js.configs.recommended],
    languageOptions: {
      sourceType: "script",
      globals: {
        ...globals.browser,
        DotCeleryDashboard: "readonly",
      },
    },
  },
  {
    files: ["src/DotCelery.Dashboard/wwwroot/dashboard-core.js"],
    languageOptions: { globals: { module: "readonly" } },
  },
  {
    files: ["eng/dashboard-ui.test.cjs"],
    extends: [js.configs.recommended],
    languageOptions: { globals: globals.node },
  },
  {
    files: [
      "*.mts",
      "eng/dashboard.browser.mts",
      "src/DotCelery.Dashboard/**/*.{ts,tsx,mts}",
    ],
    extends: [js.configs.recommended, tseslint.configs.recommendedTypeChecked],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
  {
    files: ["*.mts", "eng/dashboard.browser.mts"],
    languageOptions: { globals: globals.node },
  },
  {
    files: ["src/DotCelery.Dashboard/**/*.{ts,tsx,mts}"],
    languageOptions: { globals: globals.browser },
  },
  {
    files: ["src/DotCelery.Dashboard/Frontend/src/**/*.{ts,tsx}"],
    plugins: { "react-hooks": { rules: reactHooks.rules } },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "error",
    },
  },
  {
    files: ["src/DotCelery.Dashboard/Frontend/src/**/*.tsx"],
    extends: [reactRefresh.configs.vite],
  },
  prettier,
  {
    files: ["src/DotCelery.Dashboard/Frontend/src/shared/**/*.{ts,tsx}"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["**/features/**", "**/app/**"],
              message:
                "Shared modules must not import application or feature code.",
            },
          ],
        },
      ],
    },
  },
  {
    files: ["src/DotCelery.Dashboard/Frontend/src/features/**/*.{ts,tsx}"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["**/app/**"],
              message:
                "Features must not import application orchestration code.",
            },
          ],
        },
      ],
    },
  },
);
