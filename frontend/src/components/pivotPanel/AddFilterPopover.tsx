import type React from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { useTagValues } from "../../hooks/useTagValues";

interface AddFilterPopoverProps {
  tenantName: string;
  tagKey: string;
  activeTagFilters: string[];
  onFilterAdd: (value: string) => void;
}

export function AddFilterPopover({
  tenantName,
  tagKey,
  activeTagFilters,
  onFilterAdd,
}: AddFilterPopoverProps): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const [inputValue, setInputValue] = useState("");
  const [searchPrefix, setSearchPrefix] = useState("");
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Reset input when tagKey changes (stale text from previous key)
  useEffect(() => {
    setInputValue("");
    setSearchPrefix("");
    if (debounceRef.current) clearTimeout(debounceRef.current);
  }, [tagKey]);

  // Clear pending debounce timer on unmount
  useEffect(
    () => () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    },
    [],
  );

  const { data: values, isLoading } = useTagValues(
    tenantName,
    tagKey,
    searchPrefix || undefined,
  );

  // Exclude already-active values so duplicates cannot be added
  const options = useMemo(() => {
    const activeSet = new Set(activeTagFilters);
    return values.filter((v) => !activeSet.has(v)).map((v) => ({ value: v }));
  }, [values, activeTagFilters]);

  const handleSearchChange = (val: string) => {
    setInputValue(val);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => setSearchPrefix(val), 300);
  };

  const handleSelect = (val: string) => {
    onFilterAdd(val);
    setOpen(false);
    setInputValue("");
    setSearchPrefix("");
  };

  const handleOpenChange = (next: boolean) => {
    setOpen(next);
    if (!next) {
      setInputValue("");
      setSearchPrefix("");
    }
  };

  // Use a simple conditional render instead of antd Popover.
  // antd Popover (rc-trigger) uses internal setTimeout for popup animation,
  // which hangs with vi.useFakeTimers(), and its portal + click-outside
  // detection causes interactions inside the dropdown to unexpectedly close it.
  return (
    <div style={{ position: "relative", display: "inline-block" }}>
      {/* Native button avoids antd's rc-motion ripple setTimeout that hangs
          when vi.useFakeTimers() is active in tests */}
      <button
        type="button"
        style={{
          border: "1px dashed #d9d9d9",
          borderRadius: 6,
          padding: "0 8px",
          fontSize: 12,
          background: "transparent",
          cursor: "pointer",
          height: 24,
          lineHeight: "22px",
        }}
        onClick={() => handleOpenChange(!open)}
      >
        + Add filter
      </button>
      {open && (
        <div
          style={{
            position: "absolute",
            zIndex: 1000,
            background: "white",
            border: "1px solid #d9d9d9",
            borderRadius: 4,
            padding: 8,
            boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
            minWidth: 220,
          }}
        >
          <input
            autoFocus
            style={{ width: 200 }}
            value={inputValue}
            placeholder={`Search ${tagKey} values…`}
            onChange={(e) => handleSearchChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") handleOpenChange(false);
            }}
          />
          {options.length > 0 ? (
            options.map((opt) => (
              <div
                key={opt.value}
                role="option"
                aria-selected={false}
                style={{ cursor: "pointer", padding: "4px 8px" }}
                onClick={() => handleSelect(opt.value)}
              >
                {opt.value}
              </div>
            ))
          ) : (
            <div style={{ padding: "4px 8px", color: "#999" }}>
              {isLoading ? "Loading…" : "No values found"}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
