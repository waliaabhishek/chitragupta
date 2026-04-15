import type React from "react";
import { useState } from "react";

interface CopyLinkButtonProps {
  isDark: boolean;
}

export function CopyLinkButton({ isDark }: CopyLinkButtonProps): React.JSX.Element {
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);

  function handleClick() {
    navigator.clipboard
      .writeText(window.location.href)
      .then(() => {
        setCopied(true);
        setFailed(false);
        setTimeout(() => setCopied(false), 2000);
      })
      .catch(() => {
        setFailed(true);
        setTimeout(() => setFailed(false), 2000);
      });
  }

  return (
    <button
      onClick={handleClick}
      style={{
        background: "transparent",
        border: isDark ? "1px solid #444" : "1px solid #d9d9d9",
        borderRadius: 4,
        padding: "2px 8px",
        cursor: "pointer",
        color: failed ? "#ff4d4f" : isDark ? "#ccc" : "#555",
        fontSize: 12,
      }}
    >
      {failed ? "Failed!" : copied ? "Copied!" : "Copy link"}
    </button>
  );
}
