/*
## Description
Code for synchronising with OpenAlex.
Depends on the AWS CLI being installed and configured to be able to connect to s3://openalex/

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const zlib = require('zlib');
const fs   = require('fs');
const sh   = require('shelljs'); sh.config.verbose = false;
const json = require('../../lib/lib_json');
const base_local = require('path').resolve(__dirname + '/../../data/openalex/') + '/';
const base_remote = 's3://openalex/data/';
const aws = {
  sync   : (src,dst) => sh.exec(`aws s3 sync ${src} ${dst}`),
  ls     : (src)     => sh.exec(`aws s3 ls ${src}`).trim().split("\n").map(s => s.trim().split(' ').pop()),
  copy   : (src,dst) => sh.exec(`aws s3 cp ${src} ${dst} --recursive`),
  recent : (name)    => aws.ls(base_remote + name + '/').filter(s => s.search('updated_date=') == 0).sort().pop(),
};
const datafile = {
  load      : (i)      => json.l.decode(zlib.gunzipSync(fs.readFileSync(i)).toString()),
  save      : (o,rows) => json.l.save(o, rows.length > 0 ? rows : ''),
  transform : (i,o,fn) => datafile.save(o, datafile.load(i).map(fn).filter(v => v)),
  exists    : (f)      => fs.existsSync(f),
};
const dataset = {

  // update a dataset (synchronise and ingest)
  update : (name='works') => {
    dataset.synchronise(name);
    dataset.etl(name);
  },

  // synchronise local files in a dataset with the remote s3 bucket
  synchronise : (name='works') => {
    let dir_remote = base_remote + name + '/';
    let dir_local = base_local + name + '/';
    if (!fs.existsSync(dir_local)) { sh.mkdir('-p',dir_local); }
    aws.sync(dir_remote, dir_local);
  },

  etl : (name) => {
    let ifiles = dataset.list_files(name);
    let ofiles = [];
    let final_ofile = `${base_local}${name}/${name}.jsonlr`;

    if (datafile.exists(final_ofile)) return;

    for (let ifile of ifiles) {
      let ofile = ifile.replace('.gz','.jsonlr');
      if (!datafile.exists(ofile)) { transform(name,ifile,ofile); }
      if ( datafile.exists(ofile)) { ofiles.push(ofile); }
    }
    if (ofiles.length > 0) {
      sh.exec(`cat ${ofiles.join(' ')} > ${final_ofile}`);
      sh.rm(ofiles);
    }

    function transform(name,ifile,ofile) {
      let data = datafile.load(ifile);

      if (name == 'authors') {
        json.lr.save(ofile, data);
      }
      else if (name == 'concepts') {
        json.lr.save(ofile, data);
      }
      else if (name == 'institutions') {
        json.lr.save(ofile, data);
      }
      else if (name == 'venues') {
        json.lr.save(ofile, data);
      }
      else if (name == 'works') {
        json.lr.save(ofile, data);
      }
      else {
        console.error(`unrecognised dataset name: ${name}`);
      }
    }
  },

  // list all the gzipped files in a dataset (across multiple subdirectories)
  list_files : (name='works') => {
    let basedir = base_local + name + '/';
    let ifiles = [];

    for (let subdir of dataset.list_dirs(name)) {
      sh.ls(basedir + subdir + '/' + '*.gz').forEach(ifile => ifiles.push(ifile));
    }
    return ifiles;
  },

  // get a list of subdirectories within a dataset, sorted by age (oldest first)
  list_dirs : (name='works') => {
    let basedir = base_local + name + '/';
    return sh.ls(basedir).filter(f => f.search('updated_date=') == 0).sort();
  },

  // apply a transform function to all records in a dataset and write a single result file
  transform : (name, ofile, transform_function) => {
    let ifiles = dataset.list_files(name);
    let fd = fs.openSync(ofile,'w');

    for (let ifile of ifiles) {
      let results = datafile.load(ifile).map(transform_function).filter(v => v);
      if (results.length > 0) fs.writeSync(fd,json.l.encode(results));
    }
    fs.closeSync(fd);
  },

  // for transfer of tiles to google cloud storage, get a list of URLs
  prep_transfer : () => {
    sh.exec('aws s3 cp s3://openalex/data/works/manifest ./manifest.json');
    let f = require('./manifest.json');
    let s = ['TsvHttpData-1.0'];
    f.entries.forEach(e => {
      e.url = e.url.split('s3://openalex').join('https://openalex.s3.amazonaws.com');
      s.push(`${e.url}\t${e.meta.content_length}`);
    });
    console.log(s.join("\n"));
    sh.rm('./manifest.json');
  },

}

function extract_issns() {
  let ofile  = base_local + 'issns.jsonl';
  dataset.transform('venues', ofile, r => ({
    id : r.id,
    issnl : r.issn_l,
    issns : r.issn?.join(';') || null,
    name : r.display_name,
  }));
  let ofile2 = base_local + '../issn/issnorg.jsonlr';
  json.lr.save(ofile2, json.l.load(ofile));
}

function update_all() {
  [
    //'authors',
    //'concepts',
    //'institutions',
    'venues',
    //'works'
  ].forEach(dataset.update);
}

// synchronise a range of update batches within each dataset (last is exclusive)
function synchronise_range(first=0, last=undefined) {
  let files = list_all();
  let tree = paths_to_tree(files);
  for (let [set_name, subsets] of Object.entries(tree)) {
    for (let subset_name of Object.keys(subsets).slice(first,last)) {
      let src = `${base_remote}${set_name}/${subset_name}/`;
      let dst = `${base_local}${set_name}/${subset_name}/`;
      if (!fs.existsSync(dst)) { sh.mkdir('-p',dst); }
      aws.sync(src, dst);
    }
  }
}

// dump a specified range of JSON objects from a gz file
function extract_json_from_gz(file,first=0,last=undefined) {
  return datafile.load(file).slice(first,last);
}

// get a list of all the data files available in the S3 bucket
function list_all() {
  return (sh
    .exec(`aws s3 ls --recursive ${base_remote}`, {silent:true})
    .trim()
    .split("\n")
    .filter(s => s.search('updated_date=') != -1)
    .map(s => s.split(" data/").pop())
    .sort()
  );
}

// convert a set of paths into a tree (object)
function paths_to_tree(paths) {
  let root = {};
  paths.filter(s => s).forEach(s => path_to_tree(root,s));
  return root;
}

// given a root node and a path, convert the path into nodes in the tree
function path_to_tree(root,p) {
  let node = root;
  p.split('/').forEach(pp => {
    if (node[pp] === undefined) node[pp] = {};
    node = node[pp];
  });
}

// take a collection of jsonl.gz files and read through all the records in the set
function map_files_jsonl_gz(ifiles,func) {
  ifiles.forEach(ifile => map_file_jsonl_gz(ifile,func));
}
// given a gzipped JSONL file, map through all the records
function map_file_jsonl_gz(ifile,func) {
  datafile.load(ifile).forEach(func);
}

const MARK = Symbol();
function unmark(obj) {
  if (obj === null || typeof obj !== 'object' || obj[MARK] === undefined) return;
  delete obj[MARK];
  Object.values(obj).forEach(unmark);
}
function traverse(root,handler) {
  let objs = [root];
  let keys = ['root'];

  _traverse(root,handler);
  unmark(root);

  function _traverse(obj,func) {
    if (func(obj,objs,keys) === false) return;
    if (obj === null || typeof obj !== 'object' || obj[MARK]) return;
    obj[MARK] = true;
    objs.push(obj);
    for (let [key,val] of Object.entries(obj)) {
      keys.push(key);
      _traverse(val,func);
      keys.pop();
    }
    objs.pop();
  }
}

module.exports = { update_all, extract_issns }

if (require.main === module) {
  // list_all().forEach(item => console.log(item));
  // synchronise the works files from amazon
  // dataset.synchronise();
  // update_all();
  // extract_issns();
  // synchronise_range(0,1);
  // let num = 0;
  // let root = {};
  // map_files_jsonl_gz(dataset.list_files('venues'), r => {
  //   mst_schema(root,r);
  //   (++num % 10000 == 0) ? console.log(r) : '';
  // });
}
