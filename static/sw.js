const STATIC_CACHE = "lavert-static-v1";
const API_CACHE = "lavert-api-v1";

const STATIC_PRECACHE = [
  "/info",
  "/static/style.css",
  "/static/script.js",
  "/static/calls.js",
  "/static/crypto.js"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => cache.addAll(STATIC_PRECACHE)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => k !== STATIC_CACHE && k !== API_CACHE)
          .map((k) => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

function isApiGet(req, url) {
  return req.method === "GET" && url.origin === self.location.origin && url.pathname.startsWith("/api/");
}

function isStaticGet(req, url) {
  if (req.method !== "GET") return false;
  if (url.origin !== self.location.origin) return false;
  return (
    url.pathname === "/" ||
    url.pathname === "/info" ||
    url.pathname.startsWith("/static/") ||
    url.pathname.startsWith("/templates/")
  );
}

self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  if (isApiGet(req, url)) {
    event.respondWith(
      fetch(req)
        .then((res) => {
          if (res && res.ok) {
            const copy = res.clone();
            caches.open(API_CACHE).then((cache) => cache.put(req, copy)).catch(() => {});
          }
          return res;
        })
        .catch(() => caches.match(req))
    );
    return;
  }

  if (isStaticGet(req, url)) {
    event.respondWith(
      caches.match(req).then((cached) => {
        const network = fetch(req)
          .then((res) => {
            if (res && res.ok) {
              const copy = res.clone();
              caches.open(STATIC_CACHE).then((cache) => cache.put(req, copy)).catch(() => {});
            }
            return res;
          })
          .catch(() => cached);
        return cached || network;
      })
    );
  }
});
