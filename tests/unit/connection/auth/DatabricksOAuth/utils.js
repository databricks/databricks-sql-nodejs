function createAccessToken(expirationTime) {
  const payload = Buffer.from(JSON.stringify({ exp: expirationTime }), 'utf8').toString('base64');
  return `access.${payload}`;
}

function createValidAccessToken() {
  const expirationTime = Math.trunc(Date.now() / 1000) + 20000;
  return createAccessToken(expirationTime);
}

function createExpiredAccessToken() {
  const expirationTime = Math.trunc(Date.now() / 1000) - 1000;
  return createAccessToken(expirationTime);
}

module.exports = {
  createAccessToken,
  createValidAccessToken,
  createExpiredAccessToken,
};
