## 2025-05-23 - Accessibility of Nested Interactive Elements
**Learning:** Video cards often contain nested interactive elements (like Like buttons). While `role="link"` on the card makes it navigable, nested elements like hidden checkboxes for like buttons can trap focus or be invisible to keyboard users.
**Action:** Always ensure nested interactive elements have visible focus states (e.g., using `peer-focus-visible` on siblings of hidden inputs) and test the tab order to ensure logical navigation.
