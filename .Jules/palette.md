## 2024-05-23 - VideoPlayer Accessibility Gaps
**Learning:** The custom `VideoPlayer` component was built with rich visual features (hover effects, animations) but completely lacked accessibility attributes (`aria-label`, proper roles) for its interactive controls. This indicates a pattern where complex custom components might be missing basic a11y support.
**Action:** When working on custom interactive components in this repo, explicitly audit for screen reader support (labels, roles) as they are likely missing.
