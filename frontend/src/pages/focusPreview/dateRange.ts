function utcDate(year: number, month: number, day: number): string {
  return new Date(Date.UTC(year, month, day)).toISOString().slice(0, 10);
}

export function getCurrentUtcMonthRange(now: Date = new Date()): {
  startDate: string;
  endDate: string;
} {
  const year = now.getUTCFullYear();
  const month = now.getUTCMonth();
  return {
    startDate: utcDate(year, month, 1),
    endDate: utcDate(year, month + 1, 1),
  };
}
