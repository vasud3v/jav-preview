## 2025-02-19 - Frontend Linting Strictness
**Learning:** The frontend uses a strict custom lint rule `react-hooks/set-state-in-effect` that forbids `setState` within `useEffect`, even for standard data fetching or DOM synchronization patterns.
**Action:** When working on frontend components, prefer initializing state during the render phase or using event handlers. If `useEffect` is strictly necessary (e.g., checking DOM refs), use `// eslint-disable-next-line react-hooks/set-state-in-effect` sparingly and document why.
