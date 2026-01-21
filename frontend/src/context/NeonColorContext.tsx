import { createContext, useContext, useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useLocation } from 'react-router-dom';

interface NeonColor {
  hex: string;
  rgb: string;
  hsl: { h: number; s: number; l: number };
}

interface NeonColorContextType {
  color: NeonColor;
  complementary: NeonColor;
  palette: NeonColor[];
  regenerate: () => void;
}

const NeonColorContext = createContext<NeonColorContextType | null>(null);

// Curated neon hue ranges that produce the most vibrant, visually appealing neon colors
// Each range is [start, end] in degrees on the color wheel
const NEON_HUE_RANGES = [
  { start: 280, end: 320, name: 'magenta-pink' },    // Vibrant magentas and hot pinks
  { start: 320, end: 350, name: 'hot-pink' },        // Hot pinks and roses
  { start: 160, end: 190, name: 'cyan' },            // Electric cyans and teals
  { start: 190, end: 220, name: 'electric-blue' },   // Electric blues
  { start: 80, end: 120, name: 'lime-green' },       // Neon greens and limes
  { start: 30, end: 60, name: 'orange-yellow' },     // Neon oranges and yellows
  { start: 260, end: 280, name: 'purple' },          // Electric purples
  { start: 0, end: 20, name: 'red' },                // Neon reds
];

// Golden ratio for better distribution
const GOLDEN_RATIO = 0.618033988749895;

// Track color history to avoid repetition
let colorHistory: number[] = [];
const MAX_HISTORY = 5;

// Attempt counter for unique color generation
let attemptSeed = 0;

// Generate a random value using golden ratio for better distribution
function goldenRatioRandom(seed: number): number {
  return (seed * GOLDEN_RATIO) % 1;
}

// Check if two hues are too similar (within threshold degrees)
function huesTooSimilar(hue1: number, hue2: number, threshold = 30): boolean {
  const diff = Math.abs(hue1 - hue2);
  return Math.min(diff, 360 - diff) < threshold;
}

// HSL to RGB conversion with improved precision
function hslToRgb(h: number, s: number, l: number): { r: number; g: number; b: number } {
  const sNorm = s / 100;
  const lNorm = l / 100;
  
  const c = (1 - Math.abs(2 * lNorm - 1)) * sNorm;
  const x = c * (1 - Math.abs((h / 60) % 2 - 1));
  const m = lNorm - c / 2;
  
  let r = 0, g = 0, b = 0;
  
  if (h >= 0 && h < 60) {
    r = c; g = x; b = 0;
  } else if (h >= 60 && h < 120) {
    r = x; g = c; b = 0;
  } else if (h >= 120 && h < 180) {
    r = 0; g = c; b = x;
  } else if (h >= 180 && h < 240) {
    r = 0; g = x; b = c;
  } else if (h >= 240 && h < 300) {
    r = x; g = 0; b = c;
  } else {
    r = c; g = 0; b = x;
  }
  
  return {
    r: Math.round((r + m) * 255),
    g: Math.round((g + m) * 255),
    b: Math.round((b + m) * 255)
  };
}

// Create a NeonColor object from HSL values
function createNeonColor(h: number, s: number, l: number): NeonColor {
  const { r, g, b } = hslToRgb(h, s, l);
  const hex = `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
  const rgb = `${r},${g},${b}`;
  
  return { hex, rgb, hsl: { h, s, l } };
}

// Adjust lightness based on hue to maintain consistent perceived brightness
// Different hues have different inherent brightness (yellow appears brighter than blue)
function getAdjustedLightness(hue: number, baseLightness: number): number {
  // Hue-specific adjustments based on perceptual brightness
  // Yellow (60°) appears brightest, blue (240°) appears darkest
  const hueAdjustments: Record<string, number> = {
    yellow: -10,     // 45-75°: reduce lightness (naturally bright)
    green: -6,       // 75-165°: reduce
    cyan: -4,        // 165-195°: reduce
    blue: 2,         // 195-265°: slight increase (naturally dark)
    purple: 0,       // 265-285°: neutral
    magenta: -2,     // 285-330°: slight reduce
    red: -2,         // 330-360, 0-15°: slight reduce
    orange: -6,      // 15-45°: reduce (naturally bright)
  };
  
  let adjustment = 0;
  if (hue >= 45 && hue < 75) adjustment = hueAdjustments.yellow;
  else if (hue >= 75 && hue < 165) adjustment = hueAdjustments.green;
  else if (hue >= 165 && hue < 195) adjustment = hueAdjustments.cyan;
  else if (hue >= 195 && hue < 265) adjustment = hueAdjustments.blue;
  else if (hue >= 265 && hue < 285) adjustment = hueAdjustments.purple;
  else if (hue >= 285 && hue < 330) adjustment = hueAdjustments.magenta;
  else if (hue >= 330 || hue < 15) adjustment = hueAdjustments.red;
  else adjustment = hueAdjustments.orange;
  
  return Math.max(40, Math.min(58, baseLightness + adjustment));
}

// Generate a vibrant neon color with advanced algorithms
function generateNeonColor(): NeonColor {
  attemptSeed++;
  
  // Select a random hue range for variety
  const rangeIndex = Math.floor(goldenRatioRandom(attemptSeed + Date.now()) * NEON_HUE_RANGES.length);
  const range = NEON_HUE_RANGES[rangeIndex];
  
  // Generate hue within the selected range
  let hue: number;
  let attempts = 0;
  const maxAttempts = 10;
  
  do {
    const rangeSpan = range.end - range.start;
    const randomOffset = goldenRatioRandom(attemptSeed + attempts + Date.now() * 0.001) * rangeSpan;
    hue = (range.start + randomOffset) % 360;
    attempts++;
  } while (
    attempts < maxAttempts && 
    colorHistory.some(prevHue => huesTooSimilar(hue, prevHue))
  );
  
  // Update color history
  colorHistory.push(hue);
  if (colorHistory.length > MAX_HISTORY) {
    colorHistory.shift();
  }
  
  // High saturation for neon effect (85-98%)
  const saturation = 85 + Math.floor(goldenRatioRandom(attemptSeed + 1) * 13);
  
  // Base lightness with perceptual adjustment (lower range: 45-52%)
  const baseLightness = 45 + Math.floor(goldenRatioRandom(attemptSeed + 2) * 7);
  const lightness = getAdjustedLightness(hue, baseLightness);
  
  return createNeonColor(hue, saturation, lightness);
}

// Generate complementary color (opposite on color wheel)
function getComplementaryColor(color: NeonColor): NeonColor {
  const { h, s, l } = color.hsl;
  const complementaryHue = (h + 180) % 360;
  const adjustedLightness = getAdjustedLightness(complementaryHue, l);
  return createNeonColor(complementaryHue, s, adjustedLightness);
}

// Generate a harmonious color palette based on the primary color
function generatePalette(primary: NeonColor): NeonColor[] {
  const { h, s, l } = primary.hsl;
  
  // Triadic harmony (120° apart) with slight variations
  const triadic1Hue = (h + 120) % 360;
  const triadic2Hue = (h + 240) % 360;
  
  // Split-complementary (150° and 210° from primary)
  const split1Hue = (h + 150) % 360;
  const split2Hue = (h + 210) % 360;
  
  // Analogous colors (30° apart)
  const analogous1Hue = (h + 30) % 360;
  const analogous2Hue = (h - 30 + 360) % 360;
  
  return [
    primary,
    createNeonColor(triadic1Hue, s, getAdjustedLightness(triadic1Hue, l)),
    createNeonColor(triadic2Hue, s, getAdjustedLightness(triadic2Hue, l)),
    createNeonColor(split1Hue, s - 5, getAdjustedLightness(split1Hue, l)),
    createNeonColor(split2Hue, s - 5, getAdjustedLightness(split2Hue, l)),
    createNeonColor(analogous1Hue, s, getAdjustedLightness(analogous1Hue, l - 5)),
    createNeonColor(analogous2Hue, s, getAdjustedLightness(analogous2Hue, l - 5)),
  ];
}

export function NeonColorProvider({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const [color, setColor] = useState<NeonColor>(() => generateNeonColor());
  const lastPathRef = useRef(location.pathname);

  const complementary = useMemo(() => getComplementaryColor(color), [color]);
  const palette = useMemo(() => generatePalette(color), [color]);

  const regenerate = useCallback(() => {
    setColor(generateNeonColor());
  }, []);

  useEffect(() => {
    // Only generate new color if path actually changed (not just query params)
    if (lastPathRef.current !== location.pathname) {
      lastPathRef.current = location.pathname;
      setColor(generateNeonColor());
    }
  }, [location.pathname]);

  // Apply CSS custom properties for easy access throughout the app
  useEffect(() => {
    const root = document.documentElement;
    root.style.setProperty('--neon-color', color.hex);
    root.style.setProperty('--neon-color-rgb', color.rgb);
    root.style.setProperty('--neon-complementary', complementary.hex);
    root.style.setProperty('--neon-complementary-rgb', complementary.rgb);
    
    // Set palette colors
    palette.forEach((c, i) => {
      root.style.setProperty(`--neon-palette-${i}`, c.hex);
      root.style.setProperty(`--neon-palette-${i}-rgb`, c.rgb);
    });
  }, [color, complementary, palette]);

  return (
    <NeonColorContext.Provider value={{ color, complementary, palette, regenerate }}>
      {children}
    </NeonColorContext.Provider>
  );
}

export function useNeonColor() {
  const context = useContext(NeonColorContext);
  if (!context) {
    throw new Error('useNeonColor must be used within a NeonColorProvider');
  }
  return context;
}
