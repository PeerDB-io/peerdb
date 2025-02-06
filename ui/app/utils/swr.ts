export async function fetcher(...args: [any]) {
  const res = await fetch(...args);
  return res.json();
}
