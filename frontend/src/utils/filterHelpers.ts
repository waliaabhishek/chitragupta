export const filterByLabel = (
  input: string,
  option?: { label?: unknown },
): boolean =>
  String(option?.label ?? "")
    .toLowerCase()
    .includes(input.toLowerCase());
