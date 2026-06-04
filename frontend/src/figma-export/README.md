# Figma export – where to paste and what to share

This folder holds your Figma Make (or other) export, split **one file per screen**.

## Folder structure

| Folder | Use for |
|--------|--------|
| **screens/** | One `.tsx` (or `.jsx`) file per screen/frame from Figma. Paste or save each screen’s code here. |
| **components/** | Optional. Shared UI bits you extract (e.g. buttons, cards) that are reused across screens. |

## What to paste where

1. **screens/**  
   For each screen in your Figma design, create a file and paste that screen’s code:
   - `HomeScreen.tsx` (or similar name)
   - `RecommendationsScreen.tsx` – form for 5k time, run type, terrain, gender, foot width
   - `LeaderboardScreen.tsx` or `TrendingScreen.tsx` – trending/best trainers
   - Any other screens (e.g. results, onboarding)

2. **components/**  
   If your export has repeated pieces (buttons, inputs, cards), you can paste those here and import them in the screen files.

## When and what to share with the AI

- **When:** After you’ve pasted each screen into its own file under `screens/`.
- **What to share:**  
  - Say which screen you’re working on (e.g. “RecommendationsScreen”).  
  - Either:  
    - **Paste the file contents** of that screen, or  
    - **@-mention the file** (e.g. `@frontend/src/figma-export/screens/RecommendationsScreen.tsx`).  
  - Mention what you want next (e.g. “wire this to the recommendations API”, “match the Stridely API request shape”, “split this into smaller components”).

Example: *“Here’s my RecommendationsScreen – can you connect the form to POST /recommendations and show the results?”* and then paste or @ the file.

## Naming idea

Use clear, consistent names so we can map them to the app later:

- **Home** → `HomeScreen.tsx`
- **Get recommendations / form** → `RecommendationsScreen.tsx`
- **Trending / leaderboard / best trainers** → `LeaderboardScreen.tsx` or `TrendingScreen.tsx`
- **Results (after form submit)** → `ResultsScreen.tsx` (if you have one)

You can rename files as you go; the AI can help refactor imports.
