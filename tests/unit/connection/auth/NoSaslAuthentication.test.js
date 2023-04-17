const { expect } = require('chai');
const thrift = require('thrift');
const NoSaslAuthentication = require('../../../../dist/connection/auth/NoSaslAuthentication').default;

describe('NoSaslAuthentication', () => {
  it('auth token must be set to header', () => {
    const auth = new NoSaslAuthentication();
    const transportMock = {
      setOptions(name, value) {
        expect(name).to.be.equal('transport');
        expect(value).to.be.equal(thrift.TBufferedTransport);
      },
    };
    return auth.authenticate(transportMock).then((transport) => {
      expect(transport).to.be.eq(transportMock);
    });
  });
});
