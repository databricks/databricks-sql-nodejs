const { expect } = require('chai');
const { useAADToken } = require('../../../e2e/utils/config.local');
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
    expect(auth.useAADToken).to.be.eq(false);
  });

  it('empty password must be set', () => {
    const auth = new PlainHttpAuthentication({
      username: 'user',
      password: '',
    });

    expect(auth.username).to.be.eq('user');
    expect(auth.password).to.be.eq('');
    expect(auth.useAADToken).to.be.eq(false);
  });

  it('auth (PAT) token must be set to header', () => {
    const auth = new PlainHttpAuthentication();
    const transportMock = {
      setOptions(name, value) {
        expect(name).to.be.eq('headers');
        expect(value.Authorization).to.be.eq('Basic YW5vbnltb3VzOmFub255bW91cw==');
      },
    };
    return auth.authenticate(transportMock).then((transport) => {
      expect(transport).to.be.eq(transportMock);
    });
  });

  it('auth (Azure) token must be set to header', () => {
    const auth = new PlainHttpAuthentication({
      username: 'user',
      password: 'azureadtoken',
      useAADToken: true,
    });
    const transportMock = {
      setOptions(name, value) {
        expect(name).to.be.eq('headers');
        expect(value.Authorization).to.be.eq('Bearer azureadtoken');
      },
    };
    return auth.authenticate(transportMock).then((transport) => {
      expect(transport).to.be.eq(transportMock);
    });
  });
});
