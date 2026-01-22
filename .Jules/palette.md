## 2024-05-23 - Nested Interactive Cards
**Learning:** The application uses a "clickable card" pattern (`div` with `onClick`) that contains other interactive elements (Like Button). This creates a nested interactive control challenge for accessibility.
**Action:** Use `article` with `role="link"` for the container, manage focus manually (`tabIndex="0"`), and ensure nested controls stop propagation. Screen readers might announce "Link" for the whole card, which is acceptable if the label is clear.
