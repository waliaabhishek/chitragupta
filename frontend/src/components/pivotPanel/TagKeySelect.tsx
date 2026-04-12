import type React from "react";
import { useMemo } from "react";
import { Select, Tooltip } from "antd";
import { useTagKeys } from "../../hooks/useTagKeys";

interface TagKeySelectProps {
  tenantName: string;
  value: string;
  onChange: (key: string) => void;
}

export function TagKeySelect({
  tenantName,
  value,
  onChange,
}: TagKeySelectProps): React.JSX.Element {
  const { data: keys, isLoading, error } = useTagKeys(tenantName);

  const options = useMemo(
    () =>
      keys.length > 0
        ? keys.map((k) => ({ label: k, value: k }))
        : [{ label: "No tags configured", value: "", disabled: true }],
    [keys],
  );

  return (
    <Tooltip title={error ?? undefined}>
      <Select
        size="small"
        loading={isLoading}
        value={value || undefined}
        onChange={onChange}
        options={options}
        placeholder="Select tag key"
        style={{ width: 150 }}
      />
    </Tooltip>
  );
}
