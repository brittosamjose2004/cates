import path from 'path';
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, '.', '');
    return {
      base: './',
      build: {
        outDir: '../frontend-dist',
        emptyOutDir: true,
      },
      server: {
        port: 3000,
        host: '0.0.0.0',
        proxy: {
          '/api/': {
            target: 'http://localhost:8000',
            changeOrigin: true,
          },
        },
      },
      plugins: [react()],
      define: {
        'process.env.API_KEY': JSON.stringify(env.GEMINI_API_KEY),
        'process.env.GEMINI_API_KEY': JSON.stringify(env.GEMINI_API_KEY),
        'process.env.VITE_API_URL': JSON.stringify(env.VITE_API_URL || 'http://localhost:8000'),
      },
      resolve: {
        alias: {
          '@': path.resolve(__dirname, '.'),
        }
      }
    };
});
