// copies nested objects. note: does not copy functions.
export function deepCopy(obj) {
  const jsonStr = JSON.stringify(obj);
  return JSON.parse(jsonStr);
}
