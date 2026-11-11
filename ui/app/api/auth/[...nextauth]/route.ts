import { getAuthOptions } from '@/app/auth/options';
import NextAuth from 'next-auth';
import { cookies } from 'next/headers';

async function getHandler() {
  const cookieStore = await cookies();
  const themeCookie = cookieStore.get('peerdb-theme');
  const colorScheme = themeCookie?.value === 'dark' ? 'dark' : 'light';
  return NextAuth(getAuthOptions(colorScheme));
}

const passwordPeek = `<style>
.signin .peek-wrap { position: relative; }
.signin form button.peek {
  position: absolute;
  top: 0.25rem;
  right: 0.25rem;
  width: 2rem;
  height: 2rem;
  min-height: 0;
  margin: 0;
  padding: 0;
  border: 0;
  background: transparent;
  color: var(--color-text);
}
.signin form button.peek:hover,
.signin form button.peek:active { background: transparent; }
#input-password-for-credentials-provider { padding-right: 2.5rem; }
</style>
<script>
(function () {
  var input = document.getElementById('input-password-for-credentials-provider');
  if (!input || input.closest('.peek-wrap')) return;
  var wrap = document.createElement('div');
  wrap.className = 'peek-wrap';
  input.parentNode.insertBefore(wrap, input);
  wrap.appendChild(input);
  var eye = '<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/></svg>';
  var off = '<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3l18 18"/><path d="M10.6 10.6a3 3 0 0 0 1.4 4.4 3 3 0 0 0 3.4-1.4"/><path d="M9.9 5.2A11 11 0 0 1 12 5c6.5 0 10 7 10 7a18 18 0 0 1-3.2 4.2"/><path d="M6.1 6.1C3.5 8 2 12 2 12s3.5 7 10 7a11 11 0 0 0 4-.8"/></svg>';
  var btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'peek';
  btn.setAttribute('aria-label', 'Show password');
  btn.setAttribute('aria-pressed', 'false');
  btn.innerHTML = eye;
  btn.addEventListener('click', function () {
    var show = input.type === 'password';
    input.type = show ? 'text' : 'password';
    btn.setAttribute('aria-pressed', show ? 'true' : 'false');
    btn.setAttribute('aria-label', show ? 'Hide password' : 'Show password');
    btn.innerHTML = show ? off : eye;
    input.focus();
  });
  wrap.appendChild(btn);
})();
</script>`;

async function withPasswordPeek(request: Request, response: Response) {
  const { pathname } = new URL(request.url);
  const type = response.headers.get('content-type') ?? '';
  if (!pathname.endsWith('/signin') || !type.includes('text/html')) return response;
  const html = await response.text();
  if (!html.includes('</body>')) return new Response(html, response);
  const headers = new Headers(response.headers);
  headers.delete('content-length');
  return new Response(html.replace('</body>', passwordPeek + '</body>'), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export async function GET(
  request: Request,
  context: { params: Promise<{ nextauth: string[] }> }
) {
  const handler = await getHandler();
  return withPasswordPeek(request, await handler(request, context));
}

export async function POST(
  request: Request,
  context: { params: Promise<{ nextauth: string[] }> }
) {
  const handler = await getHandler();
  return handler(request, context);
}
