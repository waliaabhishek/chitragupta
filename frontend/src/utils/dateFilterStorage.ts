export interface StoredDates {
  start_date: string | null;
  end_date: string | null;
}

export function todayStr(): string {
  return new Date().toISOString().slice(0, 10);
}

export function thirtyDaysAgoStr(): string {
  const d = new Date();
  d.setDate(d.getDate() - 30);
  return d.toISOString().slice(0, 10);
}

export function loadDatesFromStorage(key: string): StoredDates {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return { start_date: null, end_date: null };
    return JSON.parse(raw) as StoredDates;
  } catch {
    return { start_date: null, end_date: null };
  }
}

export function saveDatesToStorage(key: string, start: string | null, end: string | null): void {
  try {
    localStorage.setItem(key, JSON.stringify({ start_date: start, end_date: end }));
  } catch {
    // localStorage unavailable — silent fail
  }
}

export function clearDatesFromStorage(key: string): void {
  try {
    localStorage.removeItem(key);
  } catch {
    // silent
  }
}
