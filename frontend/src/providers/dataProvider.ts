import type { DataProvider } from "@refinedev/core";
import { API_URL } from "../config";
import type { PaginatedResponse } from "../types/api";

/**
 * Build a tenant-scoped URL for a given resource.
 * Expects meta.tenantName to be set on list/read calls.
 */
function buildUrl(
  resource: string,
  tenantName?: string,
  id?: string | number,
): string {
  if (tenantName) {
    const base = `${API_URL}/tenants/${tenantName}/${resource}`;
    return id !== undefined ? `${base}/${id}` : base;
  }
  const base = `${API_URL}/${resource}`;
  return id !== undefined ? `${base}/${id}` : base;
}

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

export const dataProvider: DataProvider = {
  getList: async ({ resource, pagination, filters, meta }) => {
    const tenantName = meta?.tenantName as string | undefined;
    const url = new URL(buildUrl(resource, tenantName), window.location.origin);

    // Map Refine pagination to API pagination
    const current = pagination?.currentPage ?? 1;
    const pageSize = pagination?.pageSize ?? 100;
    url.searchParams.set("page", String(current));
    url.searchParams.set("page_size", String(pageSize));

    // Map filters to query params
    if (filters) {
      for (const filter of filters) {
        if ("field" in filter && "value" in filter) {
          url.searchParams.set(filter.field, String(filter.value));
        }
      }
    }

    const data = await fetchJson<PaginatedResponse<unknown>>(url.toString());
    return {
      // `as never[]` satisfies Refine's generic DataProvider return type;
      // callers receive the correctly-typed data via useList/useOne hooks.
      data: data.items as never[],
      total: data.total,
    };
  },

  getOne: async ({ resource, id, meta }) => {
    const tenantName = meta?.tenantName as string | undefined;
    const url = buildUrl(resource, tenantName, id);
    const data = await fetchJson<unknown>(url);
    // `as never` satisfies Refine's generic DataProvider return type.
    return { data: data as never };
  },

  create: async ({ resource, variables, meta }) => {
    const tenantName = meta?.tenantName as string | undefined;
    const url = buildUrl(resource, tenantName);
    const data = await fetchJson<unknown>(url, {
      method: "POST",
      body: JSON.stringify(variables),
    });
    // `as never` satisfies Refine's generic DataProvider return type.
    return { data: data as never };
  },

  update: async ({ resource, id, variables, meta }) => {
    const tenantName = meta?.tenantName as string | undefined;
    const url = buildUrl(resource, tenantName, id);
    const data = await fetchJson<unknown>(url, {
      method: "PATCH",
      body: JSON.stringify(variables),
    });
    // `as never` satisfies Refine's generic DataProvider return type.
    return { data: data as never };
  },

  deleteOne: async ({ resource, id, meta }) => {
    const tenantName = meta?.tenantName as string | undefined;
    const url = buildUrl(resource, tenantName, id);
    const data = await fetchJson<unknown>(url, { method: "DELETE" });
    // `as never` satisfies Refine's generic DataProvider return type.
    return { data: data as never };
  },

  getApiUrl: () => API_URL,
};
