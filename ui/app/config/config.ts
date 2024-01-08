export var Configuration = {
  authentication: {
    PEERDB_PASSWORD: process.env.PEERDB_PASSWORD,
    // Set this in production to a static value
    NEXTAUTH_SECRET: process.env.NEXTAUTH_SECRET,
  }
}
