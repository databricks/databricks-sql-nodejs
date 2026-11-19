import { expect } from 'chai';
import sinon from 'sinon';
import { TBinaryProtocol, TBufferedTransport, Thrift } from 'thrift';
import ThriftHttpConnection from '../../../../lib/connection/connections/ThriftHttpConnection';
import NullRetryPolicy from '../../../../lib/connection/connections/NullRetryPolicy';

describe('Thrift HTTP response boundaries', () => {
  function reply(sequenceId: number, value: string | null, method = 'Reply'): Buffer {
    let result = Buffer.alloc(0);
    const output = new TBinaryProtocol(
      new TBufferedTransport(undefined, (buffer) => {
        result = buffer as Buffer;
      }),
    );
    output.writeMessageBegin(method, Thrift.MessageType.REPLY, sequenceId);
    output.writeBool(value !== null);
    if (value !== null) output.writeString(value);
    output.writeMessageEnd();
    output.flush();
    return result;
  }

  function reader({ legacy = false, decodeError }: { legacy?: boolean; decodeError?: Error } = {}) {
    let reads = 0;
    class CountingProtocol extends TBinaryProtocol {
      readMessageBegin() {
        reads += 1;
        return super.readMessageBegin();
      }
    }
    const connection = new ThriftHttpConnection({
      url: 'http://localhost/',
      protocol: CountingProtocol,
      getRetryPolicy: async () => new NullRetryPolicy(),
    });
    const callbacks = [sinon.spy(), sinon.spy(), sinon.spy()];
    const requests: Record<number, Function> = Object.fromEntries(
      callbacks.map((callback, index) => [index + 1, callback]),
    );
    connection.client = {
      _reqs: requests,
      recv_Reply(input: TBinaryProtocol, messageType: number, sequenceId: number) {
        if (decodeError) throw decodeError;
        expect(messageType).to.equal(Thrift.MessageType.REPLY);
        const value = input.readBool() ? input.readString() : null;
        input.readMessageEnd();
        const callback = requests[sequenceId];
        delete requests[sequenceId];
        callback(null, value);
      },
    } as any;
    const receive = TBufferedTransport.receiver((input) => {
      if (legacy) Object.defineProperty(input, 'borrow', { value: undefined });
      connection['handleThriftResponse'](input);
    }, 0);
    return { callbacks, requests, receive, reads: () => reads };
  }

  for (const value of [null, '', 'a short result', 'x'.repeat(4096)]) {
    it(`does not attempt another message after a complete ${
      value === null ? 'null' : `${value.length}-byte`
    } result`, async () => {
      const input = reader();
      input.receive(reply(1, value));
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(input.callbacks[0].calledOnceWithExactly(null, value)).to.equal(true);
      expect(input.reads()).to.equal(1);
      expect(input.requests).not.to.have.property('-1');
    });
  }

  it('decodes every response in a buffer and dispatches by sequence ID', async () => {
    const input = reader();
    input.receive(Buffer.concat([reply(3, 'third'), reply(1, null), reply(2, 'second')]));
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(input.callbacks[0].calledOnceWithExactly(null, null)).to.equal(true);
    expect(input.callbacks[1].calledOnceWithExactly(null, 'second')).to.equal(true);
    expect(input.callbacks[2].calledOnceWithExactly(null, 'third')).to.equal(true);
    expect(input.reads()).to.equal(3);
    expect(input.requests).to.deep.equal({});
  });

  it('keeps the underrun handling for an empty response', () => {
    const input = reader();
    input.receive(Buffer.alloc(0));
    expect(input.reads()).to.equal(1);
    expect(input.callbacks.every((callback) => callback.notCalled)).to.equal(true);
  });

  it('rolls back a partial response and resumes without repeating completed callbacks', async () => {
    const input = reader();
    const partial = reply(2, 'second');
    input.receive(Buffer.concat([reply(1, 'first'), partial.subarray(0, partial.length - 2)]));
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(input.callbacks[0].calledOnceWithExactly(null, 'first')).to.equal(true);
    expect(input.callbacks[1].notCalled).to.equal(true);
    input.receive(partial.subarray(partial.length - 2));
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(input.callbacks[0].calledOnce).to.equal(true);
    expect(input.callbacks[1].calledOnceWithExactly(null, 'second')).to.equal(true);
  });

  it('keeps the underrun fallback for transports without borrow', async () => {
    const input = reader({ legacy: true });
    input.receive(reply(1, 'legacy'));
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(input.callbacks[0].calledOnceWithExactly(null, 'legacy')).to.equal(true);
    expect(input.reads()).to.equal(2);
  });

  it('still rejects unknown methods and removes their temporary callback', () => {
    const input = reader();
    expect(() => input.receive(reply(1, 'unexpected', 'Unknown'))).to.throw(
      'Received a response to an unknown RPC function',
    );
    expect(input.requests).not.to.have.property('-1');
    expect(input.callbacks[0].notCalled).to.equal(true);
  });

  it('does not swallow decoder errors', () => {
    const error = new Error('decoder failure');
    const input = reader({ decodeError: error });
    expect(() => input.receive(reply(1, 'value'))).to.throw(error);
  });
});
