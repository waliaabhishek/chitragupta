/**
 * Custom Vitest environment that wraps jsdom but restores native Node.js
 * AbortController/AbortSignal after jsdom setup.
 *
 * Problem: @mswjs/interceptors (used by MSW v2) creates native Request objects
 * internally. Node.js's native Request validates that init.signal is an instance
 * of the native AbortSignal class. jsdom replaces globalThis.AbortController with
 * its own implementation, so signals created in tests fail this instanceof check.
 *
 * Fix: Capture native AbortController/AbortSignal before jsdom's populateGlobal
 * installs its getter/setter, then restore them via the setter after jsdom setup.
 * Vitest's populateGlobal uses an overrideObject map for setters, so assigning
 * to global.AbortController after setup routes through the setter and wins.
 */
import type { Environment } from "vitest/runtime";
import { builtinEnvironments } from "vitest/runtime";

export default {
  name: "jsdom-native-abort",
  viteEnvironment: "client",
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async setup(global: Record<string, any>, options: Record<string, unknown>) {
    // Capture native classes BEFORE jsdom's populateGlobal installs its getter
    const NativeAbortController =
      global.AbortController as typeof AbortController;
    const NativeAbortSignal = global.AbortSignal as typeof AbortSignal;

    // Set up jsdom (installs getter/setter mechanism via populateGlobal)
    const env = await builtinEnvironments.jsdom.setup(global, options);

    // Restore native AbortController/AbortSignal via the populateGlobal setter.
    // After jsdom setup, assignment to global.AbortController triggers the
    // overrideObject.set() path, making the getter return the native class.
    global.AbortController = NativeAbortController;
    global.AbortSignal = NativeAbortSignal;

    return env;
  },
} as Environment;
