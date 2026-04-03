export function dateFormatter(params: {
  value: string | null | undefined;
}): string {
  if (!params.value) return "";
  return new Date(params.value).toLocaleDateString();
}

export function currencyFormatter(params: {
  value: string | null | undefined;
}): string {
  if (params.value == null) return "";
  return `$${Number(params.value).toFixed(2)}`;
}
