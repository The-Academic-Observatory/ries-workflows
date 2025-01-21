// Various functions to check appropriate input/output
const { ror_list, hep_map } = require("../libraries/lib_hep_map");

const check_assignment = (a) => {
  // Checks if an assignment (string) is one of 'automatic' or 'institutional'
  if (!["automatic", "institutional"].includes(a)) {
    throw Error(`Unrecognised assignment provided: ${a}`);
  }
};

const check_field = (a) => {
  // Checks if a field (string) is one of 'foe' or 'for'
  if (!["fors", "foes"].includes(a)) {
    throw Error(`Unrecognised field provided: ${a}`);
  }
};

const check_result = (result) => {
  // Throws an error if the result.success is false
  if (!result.success) {
    const message = result.message || "Operation failed. No message provided";
    throw Error(message);
  }
};

const check_hep_ror = (ror) => {
  // Throws and error if ror is not in the list of all RORs
  if (!ror_list.includes(ror)) {
    throw Error(`Unrecognised ROR provided: ${ror}`);
  }
};

const check_hep_code = (code) => {
  // Throws and error if provided code is not in the list of all HEP codes
  if (!Object.keys(hep_map).includes(code)) {
    throw Error(`Unrecognised HEP CODE provided: ${code}`);
  }
};

const check_string = (s) => {
  // Throws an error if s is not a string or is an empty string
  if (typeof s !== "string" || !s) {
    throw Error(`Invalid string provided: ${ror}`);
  }
};

module.exports = {
  check_assignment,
  check_field,
  check_result,
  check_hep_ror,
  check_hep_code,
  check_string,
};
