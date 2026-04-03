import type React from "react";
import { Descriptions, Divider, Drawer } from "antd";
import { EntityTagEditor } from "../entities/EntityTagEditor";
import type { ResourceResponse } from "../../types/api";
import { ConfluentLinkRenderer } from "../common/ConfluentLinkRenderer";

interface ResourceDetailDrawerProps {
  resource: ResourceResponse;
  tenantName: string;
  onClose: () => void;
}

export function ResourceDetailDrawer({
  resource,
  tenantName,
  onClose,
}: ResourceDetailDrawerProps): React.JSX.Element {
  return (
    <Drawer open onClose={onClose} title="Resource Details" width={480}>
      <Descriptions column={1} size="small" bordered>
        <Descriptions.Item label="Resource ID">
          <ConfluentLinkRenderer value={resource.resource_id} />
        </Descriptions.Item>
        <Descriptions.Item label="Type">
          {resource.resource_type}
        </Descriptions.Item>
        <Descriptions.Item label="Display Name">
          {resource.display_name ?? "—"}
        </Descriptions.Item>
        <Descriptions.Item label="Owner">
          {resource.owner_id ?? "—"}
        </Descriptions.Item>
        <Descriptions.Item label="Status">{resource.status}</Descriptions.Item>
      </Descriptions>
      <Divider />
      <EntityTagEditor
        tenantName={tenantName}
        entityType="resource"
        entityId={resource.resource_id}
      />
    </Drawer>
  );
}
