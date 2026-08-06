function utcDate(year: number, month: number, day: number): string {
  return new Date(Date.UTC(year, month, day)).toISOString().slice(0, 10);
}

export function getCurrentUtcMonth(now: Date = new Date()): string {
  return `${String(now.getUTCFullYear()).padStart(4, "0")}-${String(now.getUTCMonth() + 1).padStart(2, "0")}`;
}

export function getUtcMonthRange(month: string): {
  startDate: string;
  endDate: string;
} {
  const [yearText, monthText] = month.split("-");
  const year = Number(yearText);
  const zeroBasedMonth = Number(monthText) - 1;
  return {
    startDate: utcDate(year, zeroBasedMonth, 1),
    endDate: utcDate(year, zeroBasedMonth + 1, 1),
  };
}

export function getCurrentUtcMonthRange(now: Date = new Date()): {
  startDate: string;
  endDate: string;
} {
  return getUtcMonthRange(getCurrentUtcMonth(now));
}
