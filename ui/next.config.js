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
  swcMinify: true,
  output: 'standalone',
};

module.exports = nextConfig;
