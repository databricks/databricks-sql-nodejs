import { expect } from 'chai';
import PlainHttpAuthentication from '../../../../lib/connection/auth/PlainHttpAuthentication';

import ClientContextStub from '../../.stubs/ClientContextStub';

class PlainHttpAuthenticationTest extends PlainHttpAuthentication {
  public inspectInternals() {
    return {
      username: this.username,
      password: this.password,
      headers: this.headers,
    };
  }
}

describe('PlainHttpAuthentication', () => {
  it('username and password must be anonymous if nothing passed', () => {
    const auth = new PlainHttpAuthenticationTest({ context: new ClientContextStub() });

    expect(auth.inspectInternals().username).to.be.eq('anonymous');
    expect(auth.inspectInternals().password).to.be.eq('anonymous');
  });

  it('username and password must be defined correctly', () => {
    const auth = new PlainHttpAuthenticationTest({
      context: new ClientContextStub(),
      username: 'user',
      password: 'pass',
    });

    expect(auth.inspectInternals().username).to.be.eq('user');
    expect(auth.inspectInternals().password).to.be.eq('pass');
  });

  it('empty password must be set', () => {
    const auth = new PlainHttpAuthenticationTest({
      context: new ClientContextStub(),
      username: 'user',
      password: '',
    });

    expect(auth.inspectInternals().username).to.be.eq('user');
    expect(auth.inspectInternals().password).to.be.eq('');
  });

  it('auth token must be set to header', async () => {
    const auth = new PlainHttpAuthenticationTest({ context: new ClientContextStub() });
    const headers = await auth.authenticate();
    expect(headers).to.deep.equal({
      Authorization: 'Bearer anonymous',
    });
  });
});
