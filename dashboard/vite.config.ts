import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Pure client-side SPA: no SSR, no server entry, no backend.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { tsconfigPaths: true },
  server: {
    host: "127.0.0.1",
    port: 8080,
    strictPort: true,
  },
  preview: { host: "127.0.0.1", port: 8080, strictPort: true },
  build: { outDir: "dist", sourcemap: false },
});
