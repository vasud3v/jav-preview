## 2026-01-21 - Accessible Nested Interactives
**Learning:** Wrapping a card in an `<a>` tag makes nested buttons (like "Like") invalid HTML and problematic for keyboard users.
**Action:** Use a `div` with `role="link"`, `tabIndex={0}`, and `onKeyDown` for the card, and ensure nested buttons stop propagation of both `click` and relevant key events.
