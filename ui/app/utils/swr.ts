export const fetcher = (...args: [any]) =>
  fetch(...args).then((res) => res.json());
