import { expect } from 'chai';
import serializeQueryTags from '../../../lib/utils/queryTags';

describe('serializeQueryTags', () => {
  it('should return undefined for null input', () => {
    expect(serializeQueryTags(null)).to.be.undefined;
  });

  it('should return undefined for undefined input', () => {
    expect(serializeQueryTags(undefined)).to.be.undefined;
  });

  it('should return undefined for empty object', () => {
    expect(serializeQueryTags({})).to.be.undefined;
  });

  it('should serialize a single tag', () => {
    expect(serializeQueryTags({ team: 'engineering' })).to.equal('team:engineering');
  });

  it('should serialize multiple tags', () => {
    const result = serializeQueryTags({ team: 'engineering', app: 'etl' });
    expect(result).to.equal('team:engineering,app:etl');
  });

  it('should omit colon for null value', () => {
    expect(serializeQueryTags({ team: null })).to.equal('team');
  });

  it('should omit colon for undefined value', () => {
    expect(serializeQueryTags({ team: undefined })).to.equal('team');
  });

  it('should mix null and non-null values', () => {
    const result = serializeQueryTags({ team: 'eng', flag: null, app: 'etl' });
    expect(result).to.equal('team:eng,flag,app:etl');
  });

  it('should escape backslash in value', () => {
    expect(serializeQueryTags({ path: 'a\\b' })).to.equal('path:a\\\\b');
  });

  it('should escape colon in value', () => {
    expect(serializeQueryTags({ url: 'http://host' })).to.equal('url:http\\://host');
  });

  it('should escape comma in value', () => {
    expect(serializeQueryTags({ list: 'a,b' })).to.equal('list:a\\,b');
  });

  it('should escape multiple special characters in value', () => {
    expect(serializeQueryTags({ val: 'a\\b:c,d' })).to.equal('val:a\\\\b\\:c\\,d');
  });

  it('should escape backslash in key', () => {
    expect(serializeQueryTags({ 'a\\b': 'value' })).to.equal('a\\\\b:value');
  });

  it('should escape backslash in key with null value', () => {
    expect(serializeQueryTags({ 'a\\b': null })).to.equal('a\\\\b');
  });

  it('should not escape other special characters in keys', () => {
    expect(serializeQueryTags({ 'key:name': 'value' })).to.equal('key:name:value');
  });
});
