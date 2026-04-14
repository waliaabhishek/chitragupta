import { useCallback } from "react";
import { useSearchParams } from "react-router";

interface ExplorerParams {
  focus: string | null;
  at: string | null;
  tag: string | null;
  tag_value: string | null;
  diff: boolean;
  from_start: string | null;
  from_end: string | null;
  to_start: string | null;
  to_end: string | null;
}

interface UseExplorerParamsResult {
  params: ExplorerParams;
  pushParam: (key: keyof ExplorerParams, value: string | boolean | null) => void;
  pushParams: (updates: Partial<ExplorerParams>) => void;
  replaceParam: (key: keyof ExplorerParams, value: string | boolean | null) => void;
  replaceParams: (updates: Partial<ExplorerParams>) => void;
}

function readParams(searchParams: URLSearchParams): ExplorerParams {
  return {
    focus: searchParams.get("focus"),
    at: searchParams.get("at"),
    tag: searchParams.get("tag"),
    tag_value: searchParams.get("tag_value"),
    diff: searchParams.get("diff") === "true",
    from_start: searchParams.get("from_start"),
    from_end: searchParams.get("from_end"),
    to_start: searchParams.get("to_start"),
    to_end: searchParams.get("to_end"),
  };
}

function applyUpdates(
  prev: URLSearchParams,
  updates: Partial<ExplorerParams>,
): URLSearchParams {
  const next = new URLSearchParams(prev);
  for (const [k, v] of Object.entries(updates)) {
    if (v === null || v === false) {
      next.delete(k);
    } else {
      next.set(k, String(v));
    }
  }
  return next;
}

export function useExplorerParams(): UseExplorerParamsResult {
  const [searchParams, setSearchParams] = useSearchParams();

  const params = readParams(searchParams);

  const pushParam = useCallback(
    (key: keyof ExplorerParams, value: string | boolean | null) => {
      setSearchParams(
        (prev) => applyUpdates(prev, { [key]: value } as Partial<ExplorerParams>),
        { replace: false },
      );
    },
    [setSearchParams],
  );

  const pushParams = useCallback(
    (updates: Partial<ExplorerParams>) => {
      setSearchParams((prev) => applyUpdates(prev, updates), { replace: false });
    },
    [setSearchParams],
  );

  const replaceParam = useCallback(
    (key: keyof ExplorerParams, value: string | boolean | null) => {
      setSearchParams(
        (prev) => applyUpdates(prev, { [key]: value } as Partial<ExplorerParams>),
        { replace: true },
      );
    },
    [setSearchParams],
  );

  const replaceParams = useCallback(
    (updates: Partial<ExplorerParams>) => {
      setSearchParams((prev) => applyUpdates(prev, updates), { replace: true });
    },
    [setSearchParams],
  );

  return { params, pushParam, pushParams, replaceParam, replaceParams };
}
