import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,   // bind to 0.0.0.0 so Tailscale/LAN can reach it
    port: 5173,
    proxy: {
      // Proxy /api/* to the FastAPI backend (server-side, so localhost is correct)
      "/api": "http://localhost:8000",
    },
  },
});
