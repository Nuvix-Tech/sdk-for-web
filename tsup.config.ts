import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm", "iife"],
  dts: true,
  sourcemap: true,
  clean: true,
  outDir: "dist",
  noExternal: [],
  splitting: false,
  minify: false,
  target: "es2024",
  shims: true,
  bundle: true,
  skipNodeModulesBundle: true,
  tsconfig: "./tsconfig.json",
});
