const http = require('http');
const { expect } = require('chai');
const HttpTransport = require('../../../../dist/connection/transports/HttpTransport').default;

describe('HttpTransport', () => {
  it('should initialize with default options', () => {
    const transport = new HttpTransport();
    expect(transport.getOptions()).to.deep.equal({});
  });

  it('should replace all options', () => {
    const initialOptions = { a: 'a', b: 'b' };
    const transport = new HttpTransport(initialOptions);
    expect(transport.getOptions()).to.deep.equal(initialOptions);

    const newOptions = { c: 'c' };
    transport.setOptions(newOptions);
    expect(transport.getOptions()).to.deep.equal(newOptions);
  });

  it('should update only specified options', () => {
    const initialOptions = { a: 'a', b: 'b' };
    const transport = new HttpTransport(initialOptions);
    expect(transport.getOptions()).to.deep.equal(initialOptions);

    const newOptions = { b: 'new_b', c: 'c' };
    transport.updateOptions(newOptions);
    expect(transport.getOptions()).to.deep.equal({
      ...initialOptions,
      ...newOptions,
    });
  });

  it('should get specific option', () => {
    const initialOptions = { a: 'a', b: 'b' };
    const transport = new HttpTransport(initialOptions);
    expect(transport.getOptions()).to.deep.equal(initialOptions);

    expect(transport.getOption('a')).to.deep.equal(initialOptions.a);
  });

  it('should set specific option', () => {
    const initialOptions = { a: 'a', b: 'b' };
    const transport = new HttpTransport(initialOptions);
    expect(transport.getOptions()).to.deep.equal(initialOptions);

    transport.setOption('b', 'new_b');
    expect(transport.getOptions()).to.deep.equal({
      ...initialOptions,
      b: 'new_b',
    });

    transport.setOption('c', 'c');
    expect(transport.getOptions()).to.deep.equal({
      ...initialOptions,
      b: 'new_b',
      c: 'c',
    });
  });

  it('should get headers', () => {
    case1: {
      const transport = new HttpTransport();
      expect(transport.getOptions()).to.deep.equal({});

      expect(transport.getHeaders()).to.deep.equal({});
    }

    case2: {
      const initialOptions = {
        a: 'a',
        headers: { x: 'x' },
      };
      const transport = new HttpTransport(initialOptions);
      expect(transport.getOptions()).to.deep.equal(initialOptions);

      expect(transport.getHeaders()).to.deep.equal(initialOptions.headers);
    }
  });

  it('should replace headers', () => {
    const initialOptions = {
      a: 'a',
      headers: { x: 'x', y: 'y' },
    };
    const transport = new HttpTransport(initialOptions);
    expect(transport.getOptions()).to.deep.equal(initialOptions);

    const newHeaders = { y: 'new_y', z: 'z' };
    transport.setHeaders(newHeaders);
    expect(transport.getOptions()).to.deep.equal({
      ...initialOptions,
      headers: newHeaders,
    });
    expect(transport.getHeaders()).to.deep.equal(newHeaders);
  });

  it('should update only specified headers', () => {
    const initialOptions = {
      a: 'a',
      headers: { x: 'x', y: 'y' },
    };
    const transport = new HttpTransport(initialOptions);
    expect(transport.getOptions()).to.deep.equal(initialOptions);

    const newHeaders = { y: 'new_y', z: 'z' };
    transport.updateHeaders(newHeaders);
    expect(transport.getOptions()).to.deep.equal({
      ...initialOptions,
      headers: {
        ...initialOptions.headers,
        ...newHeaders,
      },
    });
    expect(transport.getHeaders()).to.deep.equal({
      ...initialOptions.headers,
      ...newHeaders,
    });
  });
});
