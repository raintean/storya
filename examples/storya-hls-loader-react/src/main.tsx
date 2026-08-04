import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import { App } from './App'
import './style.css'

const root = document.querySelector('#root')

if (!(root instanceof HTMLElement)) {
  throw new Error('HLS Loader example failed to initialize')
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
