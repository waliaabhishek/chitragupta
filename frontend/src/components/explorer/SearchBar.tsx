import type React from "react";
import { useEffect, useRef, useState } from "react";
import { useGraphSearch } from "../../hooks/useGraphSearch";
import { useDebouncedValue } from "../../hooks/useDebouncedValue";

interface SearchBarProps {
  tenantName: string | null;
  onSelect: (id: string, resourceType: string, displayName: string | null) => void;
  isDark: boolean;
}

export function SearchBar({ tenantName, onSelect, isDark }: SearchBarProps): React.JSX.Element {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const debouncedQuery = useDebouncedValue(query, 200);
  const { results, isLoading } = useGraphSearch({ tenantName, query: debouncedQuery });

  // Cmd/Ctrl+K focuses input
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        inputRef.current?.focus();
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value;
    setQuery(v);
    setOpen(v.length > 0);
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Escape") {
      setOpen(false);
    }
  }

  function handleSelect(id: string, resourceType: string, displayName: string | null) {
    onSelect(id, resourceType, displayName);
    setQuery("");
    setOpen(false);
  }

  const showDropdown = open && query.length > 0;

  return (
    <div style={{ position: "relative" }}>
      <input
        ref={inputRef}
        type="text"
        role="textbox"
        value={query}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        placeholder="Search entities… (⌘K)"
        style={{
          padding: "4px 8px",
          borderRadius: 4,
          border: isDark ? "1px solid #444" : "1px solid #d9d9d9",
          background: isDark ? "#1f1f1f" : "#fff",
          color: isDark ? "#fff" : "#000",
          width: 220,
          fontSize: 13,
        }}
      />
      {showDropdown && (
        <div
          style={{
            position: "absolute",
            top: "100%",
            left: 0,
            right: 0,
            background: isDark ? "#1f1f1f" : "#fff",
            border: isDark ? "1px solid #444" : "1px solid #d9d9d9",
            borderRadius: 4,
            zIndex: 300,
            maxHeight: 320,
            overflowY: "auto",
            boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
          }}
        >
          {isLoading && (
            <div style={{ padding: "8px 12px", opacity: 0.5, fontSize: 13 }}>
              Loading…
            </div>
          )}
          {!isLoading && results.length === 0 && (
            <div style={{ padding: "8px 12px", opacity: 0.5, fontSize: 13 }}>
              No matches found
            </div>
          )}
          {!isLoading &&
            results.map((r) => (
              <div
                key={r.id}
                onClick={() => handleSelect(r.id, r.resource_type, r.display_name)}
                style={{
                  padding: "6px 12px",
                  cursor: "pointer",
                  fontSize: 13,
                  borderBottom: isDark ? "1px solid #333" : "1px solid #f0f0f0",
                }}
              >
                <div style={{ fontWeight: 500 }}>{r.display_name ?? r.id}</div>
                <div style={{ opacity: 0.55, fontSize: 11 }}>
                  {r.resource_type}
                  {r.parent_display_name ? ` · in ${r.parent_display_name}` : ""}
                </div>
              </div>
            ))}
        </div>
      )}
    </div>
  );
}
