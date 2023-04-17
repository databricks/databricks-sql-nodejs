const http = require('http');
const { expect } = require('chai');
const HttpTransport = require('../../../../dist/connection/transports/HttpTransport').default;

describe('HttpTransport', () => {
  it('should initialize with default options', () => {
    const transport = new HttpTransport();
    expect(transport.getTransport()).to.deep.equal({});
    expect(transport.getOptions()).to.deep.equal({});
  });

  it('should handle options', () => {
    const initialOptions = { test: 'Hello, World' };

    const transport = new HttpTransport(initialOptions);
    expect(transport.getTransport()).to.deep.equal(initialOptions);
    expect(transport.getOptions()).to.deep.equal(initialOptions);

    const optionName = 'option';
    const optionValue = 123;
    const updatedOptions = {
      ...initialOptions,
      [optionName]: optionValue,
    };

    transport.setOptions(optionName, optionValue);
    expect(transport.getTransport()).to.deep.equal(updatedOptions);
    expect(transport.getOptions()).to.deep.equal(updatedOptions);
  });
});
