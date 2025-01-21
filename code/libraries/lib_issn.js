/*
## Description
Utilities for working with ISSNs

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const cache = require('../../lib/lib_cache');
const cheerio = require('cheerio');
const assert = require('assert');
const URL = 'https://portal.issn.org/resource/ISSN/{ISSN}?format=json';

function valid(code) {
  return typeof code === 'string' && null !== code.match('^[0-9A-Z]{4}-[0-9A-Z]{4}$');
}
function url(code) {
  return URL.replace('{ISSN}', code);
}
function cached(code) {
  return cache.exists(url(code));
}
async function download(code) {
  await cache.download(url(code));
}
async function load(code) {
  return await exists(code) ? parse(cache.load(url(code))) : null;
}
async function exists(code) {
  if (!valid(code)) return false;
  if (cached(code)) return true;
  await download(code);
  return cached(code);
}
function parse(txt) {
  return txt[0] == '{' ? parse_json(txt) : parse_html(txt);
}
function parse_json(txt) {
  let record = {
    issn  : '',
    issnl : '',
    title : '',
  };
  JSON.parse(txt)['@graph']?.forEach(r => {
    let tag = r['@id'].split('#')[1];
    if      (tag == 'ISSN'    ) { record.issn  = r.value; }
    else if (tag == 'ISSN-L'  ) { record.issnl = r.value; }
    else if (tag == 'KeyTitle') { record.title = r.value; }
  });
  return record;
}
function parse_html(txt) {
  let $ = cheerio.load(txt);
  let s = $('script[type="application/ld+json"]');
  let o = JSON.parse(s.html());
  return (o === null ? null : {
    issn  : o.issn,
    issnl : o.identifier.filter(v => v.name == 'ISSN-L')[0].value,
    title : o.name,
  });
}
module.exports = { valid, exists, load };

// test
if (require.main === module) {
  const assert = require('assert');

  async function test() {
    assert(valid('0000-ZZZZ'));
    assert(!valid());
    assert(!valid(100));
    assert(!valid(''));
    assert(!valid('0000-00000'));
    assert(!valid('aAAA-BBBb'));
  
    let a = await load('1793-6535');
    assert(a !== null);
    assert(a.issn   == '1793-6535');
    assert(a.issnl  == '0218-8104');

    let b = await load('0218-8104');
    assert(b !== null);
    assert(b.issn   == '0218-8104');
    assert(b.issnl  == '0218-8104');
    
    let c = await load('0000-0000');
    assert(c === null);

    console.log('tests passed');
  }
  test();
}
