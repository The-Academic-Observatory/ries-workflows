function yyyymmdd() {
  const today = new Date();
  const year  = today.getFullYear();
  const month = String(today.getMonth() + 1).padStart(2, '0'); // months are zero-based
  const day   = String(today.getDate()).padStart(2, '0');
  return `${year}${month}${day}`;
}
module.exports = yyyymmdd;