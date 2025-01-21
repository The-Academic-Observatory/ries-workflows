const hep_map = {
  ACU: {
    ror: "https://ror.org/04cxm4j25",
    name: "Australian Catholic University",
  },
  ANU: {
    ror: "https://ror.org/019wvm592",
    name: "The Australian National University",
  },
  BAT: {
    ror: "https://ror.org/03n0gvg35",
    name: "Batchelor Institute of Indigenous Tertiary Education",
  },
  BON: { ror: "https://ror.org/006jxzx88", name: "Bond University" },
  CQU: {
    ror: "https://ror.org/023q4bk22",
    name: "Central Queensland University",
  },
  CDU: {
    ror: "https://ror.org/048zcaj52",
    name: "Charles Darwin University",
  },
  CSU: {
    ror: "https://ror.org/00wfvh315",
    name: "Charles Sturt University",
  },
  CUT: { ror: "https://ror.org/02n415q13", name: "Curtin University" },
  DKN: { ror: "https://ror.org/02czsnj07", name: "Deakin University" },
  ECU: {
    ror: "https://ror.org/05jhnwe22",
    name: "Edith Cowan University",
  },
  FED: {
    ror: "https://ror.org/05qbzwv83",
    name: "Federation University",
  },
  FLN: {
    ror: "https://ror.org/01kpzv902",
    name: "Flinders University",
  },
  GRF: {
    ror: "https://ror.org/02sc3r913",
    name: "Griffith University",
  },
  JCU: {
    ror: "https://ror.org/04gsp2c11",
    name: "James Cook University",
  },
  LTU: {
    ror: "https://ror.org/01rxfrp27",
    name: "La Trobe University",
  },
  MQU: {
    ror: "https://ror.org/01sf06y89",
    name: "Macquarie University",
  },
  MON: { ror: "https://ror.org/02bfwt286", name: "Monash University" },
  MUR: { ror: "https://ror.org/00r4sry34", name: "Murdoch University" },
  QUT: {
    ror: "https://ror.org/03pnv4752",
    name: "Queensland University of Technology",
  },
  RMT: {
    ror: "https://ror.org/04ttjf776",
    name: "Royal Melbourne Institute of Technology",
  },
  SCU: {
    ror: "https://ror.org/001xkv632",
    name: "Southern Cross University",
  },
  SWN: {
    ror: "https://ror.org/031rekg67",
    name: "Swinburne University of Technology",
  },
  TOR: {
    ror: "https://ror.org/0351xae06",
    name: "Torrens University Australia",
  },
  NSW: {
    ror: "https://ror.org/03r8z3t63",
    name: "University of New South Wales",
  },
  ADE: {
    ror: "https://ror.org/00892tw58",
    name: "University of Adelaide",
  },
  CAN: {
    ror: "https://ror.org/04s1nv328",
    name: "University of Canberra",
  },
  DIV: {
    ror: "https://ror.org/02xn8bh65",
    name: "University of Divinity",
  },
  MEL: {
    ror: "https://ror.org/01ej9dk98",
    name: "University of Melbourne",
  },
  UNE: {
    ror: "https://ror.org/04r659a56",
    name: "University of New England",
  },
  NEW: {
    ror: "https://ror.org/00eae9z71",
    name: "University of Newcastle",
  },
  NDA: {
    ror: "https://ror.org/02stey378",
    name: "University of Notre Dame Australia",
  },
  QLD: {
    ror: "https://ror.org/00rqy9422",
    name: "University of Queensland",
  },
  USA: {
    ror: "https://ror.org/01p93h210",
    name: "University of South Australia",
  },
  USQ: {
    ror: "https://ror.org/04sjbnx57",
    name: "University of Southern Queensland",
  },
  SYD: {
    ror: "https://ror.org/0384j8v12",
    name: "University of Sydney",
  },
  TAS: {
    ror: "https://ror.org/01nfmeh72",
    name: "University of Tasmania",
  },
  UTS: {
    ror: "https://ror.org/03f0f6041",
    name: "University of Technology, Sydney",
  },
  UWA: {
    ror: "https://ror.org/047272k79",
    name: "University of Western Australia",
  },
  WOL: {
    ror: "https://ror.org/00jtmb277",
    name: "University of Wollongong",
  },
  USC: {
    ror: "https://ror.org/016gb9e15",
    name: "University of the Sunshine Coast",
  },
  VIC: {
    ror: "https://ror.org/04j757h98",
    name: "Victoria University",
  },
  WSU: {
    ror: "https://ror.org/03t52dk35",
    name: "Western Sydney University",
  },
};

const ror_list = Object.values(hep_map).map((hep) => hep.ror);

module.exports = { hep_map, ror_list };
