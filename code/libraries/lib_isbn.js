/*
## Description
Some helpers for working with ISBN 10 and 13 values

For the checksum methods, see: https://en.wikipedia.org/wiki/ISBN#ISBN-10_check_digits

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/

const { assert } = require('console');

// calculate the ISBN10 check value
function check10(s='') {
  const csum = [10,9,8,7,6,5,4,3,2].reduce((agg,val,key) => agg + val * +s[key], 0);
  const cval = (11 - csum % 11) % 11;
  return cval == 10 ? 'X' : String(cval);
}

// calculate the ISBN13 check value
function check13(s='') {
  const csum = [1,3,1,3,1,3,1,3,1,3,1,3].reduce((agg,val,key) => agg + val * +s[key], 0);
  return String((10-(csum % 10)) % 10);
}

// upgrade an ISBN10 to an ISBN13
function upgrade(s='') {
  s = '978' + s.slice(0,9);
  return s + check13(s);
}

function valid10(s='') {
  return /^[0-9]{9}[0-9X]$/.test(s) && s[9] == check10(s);
}

function valid13(s='') {
  return /^[0-9]{13}$/.test(s) && s[12] == check13(s);
}

function parse(s='') {
  s = s.toUpperCase().replaceAll(/[^0-9X]/gi,'');
  return valid10(s) ? upgrade(s) : valid13(s) ? s : '';
}
module.exports = parse;

//test
if (require.main === module) {
  const a = require('assert');
  a(check10('0306406152') === '2');
  a(check10('0306406162') !== '2');
  
  // test valid ISBN13
  [
    '978-1-60309-517-4',
    '978-1-60309-520-4',
    '978-1-60309-511-2',
    '978-1-60309-508-2',
    '978-1-60309-515-0',
    '978-1-60309-521-1',
    '978-1-60309-522-8',
    '978-1-60309-519-8',
    '978-1-60309-516-7',
  ].forEach(raw => {
    let str = parse(raw);
    assert(raw.replaceAll('-','') === str);
    assert(check13(str) === raw.split('-').pop());
  });
}

