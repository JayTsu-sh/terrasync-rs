/**
 * Check a fetch Response and throw a descriptive Error if not ok.
 *
 * Tries to extract `{ error: "..." }` from the response body (the format
 * produced by the backend's `WebError::into_response`).  Falls back to raw
 * text so we never silently swallow non-JSON error pages.
 */
export async function throwIfError(res: Response): Promise<void> {
  if (res.ok) return
  const text = await res.text()
  let msg: string
  try {
    msg = JSON.parse(text).error || text
  } catch {
    msg = text
  }
  throw new Error(msg)
}
