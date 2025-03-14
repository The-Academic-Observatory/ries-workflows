/*
## Summary
Exports datafiles for use with the RIES dashboard. One CSV for FoRs, another for FoEs. 

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0
*/
const compile = ({
  project = "",
  dataset = "",
  filename = "",
  replace = false,
  version = "",
}) => `
-- generated by: ${require("path").basename(__filename)}
BEGIN
  EXPORT DATA OPTIONS (
    uri         = 'gs://${project}-ries/tables/dashboard/${filename}_*.csv',
    format      = 'CSV',
    header      = true,
    overwrite   = ${replace}
  ) AS SELECT * FROM \`${project}.${dataset}.heps_gui_table${version}\`;

  -- camel case for the newer dashboard
  EXPORT DATA OPTIONS (
    uri         = 'gs://${project}-ries/tables/dashboard/${filename}_cc_*.csv',
    format      = 'CSV',
    header      = true,
    overwrite   = ${replace}
  ) AS (
    SELECT 
      idx_hep       AS inst,
      IF(idx_for='MD','99',idx_for) AS conc,
      idx_year      AS year,
      rci_local     AS rciLocal,
      rci_world     AS rciGlobal,
      hpi_world     AS hpiGlobal,
      sum_papers    AS totalOutputs,
      sum_citations AS totalCitations
    FROM \`${project}.${dataset}.heps_gui_table${version}\`
  );

END;
`;
module.exports = compile;
