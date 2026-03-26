import type React from "react";
import { Descriptions, Divider, Drawer } from "antd";
import { EntityTagEditor } from "../entities/EntityTagEditor";
import type { IdentityResponse } from "../../types/api";

interface IdentityDetailDrawerProps {
  identity: IdentityResponse;
  tenantName: string;
  onClose: () => void;
}

export function IdentityDetailDrawer({
  identity,
  tenantName,
  onClose,
}: IdentityDetailDrawerProps): React.JSX.Element {
  return (
    <Drawer open onClose={onClose} title="Identity Details" width={480}>
      <Descriptions column={1} size="small" bordered>
        <Descriptions.Item label="Identity ID">{identity.identity_id}</Descriptions.Item>
        <Descriptions.Item label="Type">{identity.identity_type}</Descriptions.Item>
        <Descriptions.Item label="Display Name">{identity.display_name ?? "—"}</Descriptions.Item>
        <Descriptions.Item label="Status">{identity.deleted_at ? "Deleted" : "Active"}</Descriptions.Item>
      </Descriptions>
      <Divider />
      <EntityTagEditor
        tenantName={tenantName}
        entityType="identity"
        entityId={identity.identity_id}
      />
    </Drawer>
  );
}
