import { expect } from 'chai';
import sinon from 'sinon';
import reydenCache from '../../../lib/ReydenWarehouseCache';
import ThriftBackend from '../../../lib/thrift-backend/ThriftBackend';
import StatusError from '../../../lib/errors/StatusError';
import { TStatusCode } from '../../../thrift/TCLIService_types';

describe('Reyden Warehouse Cache', () => {
  beforeEach(() => {
    reydenCache.clear();
  });

  afterEach(() => {
    reydenCache.clear();
  });

  describe('Warehouse ID Extraction', () => {
    it('should extract warehouse ID from /warehouses/<id> path', () => {
      const extractWarehouseId = (ThriftBackend as any).extractWarehouseId;
      expect(extractWarehouseId('/sql/1.0/warehouses/abc123')).to.equal('abc123');
    });

    it('should extract endpoint ID from /endpoints/<id> path', () => {
      const extractWarehouseId = (ThriftBackend as any).extractWarehouseId;
      expect(extractWarehouseId('/sql/1.0/endpoints/xyz789')).to.equal('xyz789');
    });

    it('should stop at query string when extracting warehouse ID', () => {
      const extractWarehouseId = (ThriftBackend as any).extractWarehouseId;
      expect(extractWarehouseId('/sql/1.0/warehouses/abc123?o=12345')).to.equal('abc123');
    });

    it('should return undefined if no warehouse ID is found', () => {
      const extractWarehouseId = (ThriftBackend as any).extractWarehouseId;
      expect(extractWarehouseId('/some/other/path')).to.be.undefined;
    });

    it('should return undefined for undefined path', () => {
      const extractWarehouseId = (ThriftBackend as any).extractWarehouseId;
      expect(extractWarehouseId(undefined)).to.be.undefined;
    });
  });

  describe('Cache Operations', () => {
    it('should mark a warehouse as Reyden', () => {
      const host = 'example.com';
      const warehouseId = 'warehouse-123';

      expect(reydenCache.isKnownReyden(host, warehouseId)).to.be.undefined;
      reydenCache.markReyden(host, warehouseId);
      expect(reydenCache.isKnownReyden(host, warehouseId)).to.be.true;
    });

    it('should be case-insensitive on host', () => {
      const warehouseId = 'warehouse-123';

      reydenCache.markReyden('Example.COM', warehouseId);

      expect(reydenCache.isKnownReyden('example.com', warehouseId)).to.be.true;
      expect(reydenCache.isKnownReyden('EXAMPLE.COM', warehouseId)).to.be.true;
    });

    it('should isolate entries by warehouse ID', () => {
      const host = 'example.com';

      reydenCache.markReyden(host, 'warehouse-1');

      expect(reydenCache.isKnownReyden(host, 'warehouse-1')).to.be.true;
      expect(reydenCache.isKnownReyden(host, 'warehouse-2')).to.be.undefined;
    });

    it('should isolate entries by host', () => {
      const warehouseId = 'warehouse-123';

      reydenCache.markReyden('host1.com', warehouseId);

      expect(reydenCache.isKnownReyden('host1.com', warehouseId)).to.be.true;
      expect(reydenCache.isKnownReyden('host2.com', warehouseId)).to.be.undefined;
    });

    it('should have cache size method', () => {
      expect(reydenCache.size()).to.equal(0);

      reydenCache.markReyden('host1.com', 'warehouse-1');
      expect(reydenCache.size()).to.equal(1);

      reydenCache.markReyden('host1.com', 'warehouse-2');
      expect(reydenCache.size()).to.equal(2);
    });

    it('should clear cache', () => {
      reydenCache.markReyden('host1.com', 'warehouse-1');
      reydenCache.markReyden('host2.com', 'warehouse-2');
      expect(reydenCache.size()).to.equal(2);

      reydenCache.clear();
      expect(reydenCache.size()).to.equal(0);
      expect(reydenCache.isKnownReyden('host1.com', 'warehouse-1')).to.be.undefined;
    });
  });

  describe('Cache TTL Expiry', () => {
    let clock: sinon.SinonFakeTimers;

    beforeEach(() => {
      clock = sinon.useFakeTimers();
    });

    afterEach(() => {
      clock.restore();
    });

    it('keeps an entry until the 6h TTL, then evicts it on access', () => {
      const host = 'example.com';
      const warehouseId = 'warehouse-ttl';
      const sixHoursMs = 6 * 60 * 60 * 1000;

      reydenCache.markReyden(host, warehouseId);
      expect(reydenCache.isKnownReyden(host, warehouseId)).to.be.true;

      // At exactly the TTL boundary the entry is still valid (strict >).
      clock.tick(sixHoursMs);
      expect(reydenCache.isKnownReyden(host, warehouseId)).to.be.true;

      // One tick past the TTL: expired, evicted on access.
      clock.tick(1);
      expect(reydenCache.isKnownReyden(host, warehouseId)).to.be.undefined;
      expect(reydenCache.size()).to.equal(0);
    });

    it('sweeps expired entries when a new warehouse is marked', () => {
      const sixHoursMs = 6 * 60 * 60 * 1000;

      reydenCache.markReyden('host-a.com', 'warehouse-a');
      expect(reydenCache.size()).to.equal(1);

      // Advance past the TTL so the first entry is expired, then mark a second
      // warehouse. markReyden sweeps the expired entry rather than only adding —
      // proven by the size dropping back to 1 without warehouse-a ever being
      // looked up (a lookup would otherwise trigger the lazy per-key eviction).
      clock.tick(sixHoursMs + 1);
      reydenCache.markReyden('host-b.com', 'warehouse-b');

      expect(reydenCache.size()).to.equal(1);
      expect(reydenCache.isKnownReyden('host-b.com', 'warehouse-b')).to.be.true;
    });
  });
});

describe('StatusError SQLSTATE Support', () => {
  it('should capture SQLSTATE in StatusError', () => {
    const error = new StatusError({
      statusCode: TStatusCode.ERROR_STATUS,
      errorMessage: 'Some error',
      sqlState: 'KP001',
    });

    expect(error.sqlState).to.equal('KP001');
  });

  it('should handle undefined SQLSTATE', () => {
    const error = new StatusError({
      statusCode: TStatusCode.ERROR_STATUS,
      errorMessage: 'Some error',
    });

    expect(error.sqlState).to.be.undefined;
  });

  it('should detect KP001 errors correctly', () => {
    const kp001Error = new StatusError({
      statusCode: TStatusCode.ERROR_STATUS,
      errorMessage: 'Lakehouse/RT is not supported for Thrift protocol',
      sqlState: 'KP001',
    });

    expect(kp001Error.sqlState).to.equal('KP001');
  });
});
