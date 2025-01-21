/*
## Description
Helper functions for working with Objects

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
function zip(keys,vals) {
  return Object.fromEntries(keys.map((k,i) => [k,vals[i]]));
}
function deep_copy_json(obj) {
  return JSON.parse(JSON.stringify(obj));
}
function deep_copy(value) {
  if (value === null || typeof value !== 'object') { return value }
  if (Array.isArray(value)) { return value.map(deep_copy); }
  return Object.fromEntries(Object.entries(value).map(([key,val]) => [key,deep_copy(val)]));
}
function deep_check(args,defs,parent='') {
  for (let [k,v] of Object.entries(defs)) {
    if (args[k] === undefined) continue;
    if (v.constructor !== args[k].constructor) return `${parent}${k}`;
    if (typeof v === 'object') {
      let err = deep_check(args[k],v,k+'.');
      if (err) return err;
    }
  }
  return '';
}
function deep_assign(base,other) {
  if (other === undefined || other === null || typeof other !== 'object') return other;
  if (base  === undefined || base  === null || typeof base  !== 'object') return deep_copy(other);
  if (base.constructor != other.constructor) return deep_copy(other);
  for (let [k,v] of Object.entries(other)) {
    base[k] = deep_assign(base[k],v);
    if (base[k] === undefined) delete base[k];
  }
  return base;
}

// Traverse an object
const TAG = Symbol('TAG');
function traverse(root={}, handler) {
  const stack = [{key:'root',val:root}];

  function visit(node) {
    if (node && typeof node === 'object' && !node[TAG]) {
      node[TAG] = true;
      if (handler(node,stack) !== true) {
        for (let [key,val] of Object.entries(node)) {
          stack.push({key,val});
          visit(val);
          stack.pop();
        }
      }
    }
  }
  function leave(node) {
    if (node && typeof node === 'object' && node[TAG]) {
      delete node[TAG];
      for (let key of Object.keys(node)) { leave(node[key]); }
    }
  }
  visit(root);
  leave(root);
}
//traverse({a:{b:{c:'c'}}}, (v,s) => console.log(v,s))

function handler(stack, node, depth) {
  console.log('Stack:', stack);
  console.log('Node:', node);
  console.log('Depth:', depth);
  // Continue traversal
  return true;
}

function map(obj,func) {
  return Object.fromEntries(Object.entries(obj).map(kp => func(kp[0],kp[1])).filter(v=>v && v[1] !== undefined));
}
module.exports = { zip, deep_copy, deep_check, deep_assign };
