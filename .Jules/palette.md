# Palette's Journal

## 2024-05-22 - Invisible Checkbox Pattern Accessibility
**Learning:** The "invisible checkbox" pattern for toggle buttons (like the Like button) creates a severe accessibility trap. While it works for mouse users, setting `opacity: 0` on the input removes default focus indicators, leaving keyboard users with no visual feedback when they've navigated to the control.
**Action:** Always pair the hidden checkbox with a `peer` class and use `peer-focus-visible` on the visible sibling element to restore the focus ring.
