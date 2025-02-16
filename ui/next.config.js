/** @type {import('next').NextConfig} */
const nextConfig = {
  compiler: {
    styledComponents: true,
  },
  async redirects() {
    return [
      {
        source: '/',
        destination: '/peers',
        permanent: false,
      },
    ];
  },
  reactStrictMode: true,
  output: 'standalone',
  images: {
    unoptimized: true,
  },
};

module.exports = nextConfig;
