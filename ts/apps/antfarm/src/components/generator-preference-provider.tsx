import type { GeneratorConfig } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useCallback, useMemo, useState } from "react";
import { GeneratorPreferenceContext } from "@/contexts/generator-preference-context";

const STORAGE_KEY = "antfarm-dashboard-generator";

function loadStoredGenerator(): GeneratorConfig | null {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw);
    if (
      parsed &&
      typeof parsed === "object" &&
      typeof parsed.provider === "string" &&
      typeof parsed.model === "string"
    ) {
      return parsed as GeneratorConfig;
    }
  } catch {
    localStorage.removeItem(STORAGE_KEY);
  }

  return null;
}

export function GeneratorPreferenceProvider({ children }: { children: ReactNode }) {
  const [dashboardGeneratorState, setDashboardGeneratorState] = useState<GeneratorConfig | null>(
    loadStoredGenerator
  );

  const setDashboardGenerator = useCallback((value: GeneratorConfig | null) => {
    setDashboardGeneratorState(value);

    if (value) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
    } else {
      localStorage.removeItem(STORAGE_KEY);
    }
  }, []);

  const contextValue = useMemo(
    () => ({
      dashboardGenerator: dashboardGeneratorState,
      setDashboardGenerator,
    }),
    [dashboardGeneratorState, setDashboardGenerator]
  );

  return (
    <GeneratorPreferenceContext.Provider value={contextValue}>
      {children}
    </GeneratorPreferenceContext.Provider>
  );
}
