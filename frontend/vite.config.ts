import path from "path"
import { fileURLToPath } from "url"
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const __filepath = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filepath);

console.log("Vite Config - __dirname:", __dirname);
console.log("Vite Config - cwd:", process.cwd());

export default defineConfig({
  root: __dirname,
  plugins: [
    react({
      // Enable Fast Refresh
      fastRefresh: true,
    }), 
    tailwindcss()
  ],
  build: {
    // Target modern browsers for smaller bundles
    target: 'es2020',
    rollupOptions: {
      input: path.resolve(__dirname, 'index.html'),
      output: {
        // Optimized code splitting
        manualChunks: {
          // Core React libraries
          'react-vendor': ['react', 'react-dom'],
          // Router separate for better caching
          'router': ['react-router-dom'],
          // Video players (large dependencies)
          'video-vendor': ['hls.js'],
          // Icons (can be large)
          'icons': ['lucide-react'],
        },
        // Optimize chunk names for better caching
        chunkFileNames: 'assets/[name]-[hash].js',
        entryFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash].[ext]',
      },
    },
    // Increase chunk size warning limit
    chunkSizeWarningLimit: 1000,
    // Use esbuild for faster minification
    minify: 'esbuild',
    // Enable CSS code splitting
    cssCodeSplit: true,
    // Source maps only for errors (smaller build)
    sourcemap: false,
    // Optimize dependencies
    commonjsOptions: {
      transformMixedEsModules: true,
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
    extensions: ['.mjs', '.js', '.mts', '.ts', '.jsx', '.tsx', '.json']
  },
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
  // Optimize dependencies
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      'react-router-dom',
      'hls.js',
      'lucide-react'
    ],
    // Exclude large dependencies that don't need pre-bundling
    exclude: ['video.js', '@videojs/http-streaming'],
  },
  // Performance optimizations
  esbuild: {
    // Remove debugger statements in production
    drop: import.meta.env?.MODE === 'production' ? ['debugger', 'console'] : [],
    // Optimize for speed
    legalComments: 'none',
  },
})
