import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { AuthProvider } from './context/AuthContext'

// Graceful shutdown handling
window.addEventListener('beforeunload', (event) => {
  // Cleanup any pending requests, close connections, etc.
  console.log('Application shutting down gracefully...')
  
  // You can add cleanup logic here if needed
  // For example: close WebSocket connections, cancel pending requests, etc.
})

// Handle page visibility changes (tab switching, minimizing)
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    console.log('Application paused (tab hidden)')
  } else {
    console.log('Application resumed (tab visible)')
  }
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AuthProvider>
      <App />
    </AuthProvider>
  </StrictMode>,
)
