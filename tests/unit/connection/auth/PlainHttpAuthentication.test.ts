import { expect } from 'chai';
import PlainHttpAuthentication from '../../../../lib/connection/auth/PlainHttpAuthentication';

import ClientContextStub from '../../.stubs/ClientContextStub';

describe('PlainHttpAuthentication', () => {
  it('username and password must be anonymous if nothing passed', () => {
    const auth = new PlainHttpAuthentication({ context: new ClientContextStub() });

    expect(auth['username']).to.be.eq('anonymous');
    expect(auth['password']).to.be.eq('anonymous');
  });

  it('username and password must be defined correctly', () => {
    const auth = new PlainHttpAuthentication({
      context: new ClientContextStub(),
      username: 'user',
      password: 'pass',
    });

    expect(auth['username']).to.be.eq('user');
    expect(auth['password']).to.be.eq('pass');
  });

  it('empty password must be set', () => {
    const auth = new PlainHttpAuthentication({
      context: new ClientContextStub(),
      username: 'user',
      password: '',
    });

    expect(auth['username']).to.be.eq('user');
    expect(auth['password']).to.be.eq('');
  });

  it('auth token must be set to header', async () => {
    const auth = new PlainHttpAuthentication({ context: new ClientContextStub() });
    const headers = await auth.authenticate();
    expect(headers).to.deep.equal({
      Authorization: 'Bearer anonymous',
    });
  });
});
