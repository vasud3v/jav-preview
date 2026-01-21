import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { AuthProvider } from './context/AuthContext'

// Performance monitoring
if (import.meta.env.DEV) {
  // Only in development
  console.log('🚀 App starting in development mode');
}

// Preload critical resources
const preloadCriticalResources = () => {
  // Preload fonts if any
  // const fontLink = document.createElement('link');
  // fontLink.rel = 'preload';
  // fontLink.as = 'font';
  // fontLink.href = '/fonts/your-font.woff2';
  // document.head.appendChild(fontLink);
};

preloadCriticalResources();

// Optimize rendering
const rootElement = document.getElementById('root');
if (!rootElement) throw new Error('Root element not found');

// Use concurrent features for better performance
const root = createRoot(rootElement);

root.render(
  <StrictMode>
    <AuthProvider>
      <App />
    </AuthProvider>
  </StrictMode>,
);

// Report Web Vitals (optional - for monitoring)
if (import.meta.env.PROD) {
  // You can add web vitals reporting here
  // import('web-vitals').then(({ getCLS, getFID, getFCP, getLCP, getTTFB }) => {
  //   getCLS(console.log);
  //   getFID(console.log);
  //   getFCP(console.log);
  //   getLCP(console.log);
  //   getTTFB(console.log);
  // });
}
