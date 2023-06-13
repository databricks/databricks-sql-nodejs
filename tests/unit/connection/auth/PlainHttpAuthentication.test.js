const { expect } = require('chai');
const PlainHttpAuthentication = require('../../../../dist/connection/auth/PlainHttpAuthentication').default;

describe('PlainHttpAuthentication', () => {
  it('username and password must be anonymous if nothing passed', () => {
    const auth = new PlainHttpAuthentication();

    expect(auth.username).to.be.eq('anonymous');
    expect(auth.password).to.be.eq('anonymous');
  });

  it('username and password must be defined correctly', () => {
    const auth = new PlainHttpAuthentication({
      username: 'user',
      password: 'pass',
    });

    expect(auth.username).to.be.eq('user');
    expect(auth.password).to.be.eq('pass');
  });

  it('empty password must be set', () => {
    const auth = new PlainHttpAuthentication({
      username: 'user',
      password: '',
    });

    expect(auth.username).to.be.eq('user');
    expect(auth.password).to.be.eq('');
  });

  it('auth token must be set to header', async () => {
    const auth = new PlainHttpAuthentication();
    const transportMock = {
      updateHeaders(headers) {
        expect(headers).to.deep.equal({
          Authorization: 'Bearer anonymous',
        });
      },
    };
    await auth.authenticate(transportMock); // it just should not fail
  });
});
