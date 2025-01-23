/*
## Summary
Intercept a RIES run to convert existing core_papers, core_assignments and core_fors tables to use 
FoEs instead of FoRs.

## Description
Existing FoR assignments are mapped into FoE assignments using a custom JS function in BigQuery.
The original mapping was provided to us by UNE. An additional mapping was auto-generated from 
government-sourced data and turned out to be a good match for the UNE mapping. For now, this code 
is still using the UNE mapping.

## Contacts
julian.tonti-filippini@curtin.edu.au

## License
Apache 2.0

## Requires
core_fors
core_papers
core_assignments
translate_fors_to_foes_src

## Creates
core_fors
core_papers
core_assignments

*/
//better alternative, not used for now
//const func_src = require('./translate_fors_to_foes_src.js').toString().split('//<template>')[1];
const compile = ({
  project = "",
  dataset = "",
  replace = false,
  version = "",
}) => `
-- generated by: ${require("path").basename(__filename)}
CREATE SCHEMA IF NOT EXISTS \`${project}.${dataset}\`;

BEGIN

  -- this function converts an FoR assignment into an FoE assignment
  CREATE TEMPORARY FUNCTION fors_to_foes(apportionments ARRAY<STRUCT<
    vers STRING,
    len INTEGER,
    code STRING,
    name STRING,
    weight INTEGER
  >>) 
  RETURNS ARRAY<STRUCT<
    vers STRING,
    len INTEGER,
    code STRING,
    name STRING,
    weight INTEGER
  >>
  LANGUAGE js
  AS r"""
    const for_to_foe_map = {
      "30":"05",
      "31":"01",
      "32":"06",
      "33":"04",
      "34":"01",
      "35":"08",
      "36":"10",
      "37":"01",
      "38":"09",
      "39":"07",
      "40":"03",
      "41":"05",
      "42":"06",
      "43":"09",
      "44":"09",
      "45":"09",
      "46":"02",
      "47":"09",
      "48":"09",
      "49":"01",
      "50":"09",
      "51":"01",
      "52":"09",
      "99":"99",
      "MD":"99",
      "3001":"0501",
      "3002":"0501",
      "3003":"0501",
      "3004":"0501",
      "3005":"0507",
      "3006":"0199",
      "3007":"0505",
      "3008":"0503",
      "3009":"0611",
      "3099":"0599",
      "3101":"0109",
      "3102":"0109",
      "3103":"0109",
      "3104":"0109",
      "3105":"0109",
      "3106":"0199",
      "3107":"0109",
      "3108":"0109",
      "3109":"0109",
      "3199":"0109",
      "3201":"0601",
      "3202":"0601",
      "3203":"0607",
      "3204":"0601",
      "3205":"0199",
      "3206":"0199",
      "3207":"0199",
      "3208":"0109",
      "3209":"0601",
      "3210":"0699",
      "3211":"0601",
      "3212":"0609",
      "3213":"0601",
      "3214":"0605",
      "3215":"0601",
      "3299":"0699",
      "3301":"0401",
      "3302":"0401",
      "3303":"0401",
      "3304":"0401",
      "3399":"0401",
      "3401":"0105",
      "3402":"0105",
      "3403":"0105",
      "3404":"0105",
      "3405":"0105",
      "3406":"0105",
      "3407":"0105",
      "3499":"0105",
      "3501":"0801",
      "3502":"0811",
      "3503":"0803",
      "3504":"0803",
      "3505":"0803",
      "3506":"0805",
      "3507":"0803",
      "3508":"0807",
      "3509":"0899",
      "3599":"0899",
      "3601":"1099",
      "3602":"1001",
      "3603":"1001",
      "3604":"1001",
      "3605":"1007",
      "3606":"1003",
      "3699":"1099",
      "3701":"0107",
      "3702":"0107",
      "3703":"0107",
      "3704":"0107",
      "3705":"0107",
      "3706":"0107",
      "3707":"0107",
      "3708":"0107",
      "3709":"0107",
      "3799":"0107",
      "3801":"0919",
      "3802":"0919",
      "3803":"0919",
      "3899":"0919",
      "3901":"0703",
      "3902":"0799",
      "3903":"0701",
      "3904":"0799",
      "3999":"0799",
      "4001":"0315",
      "4002":"0305",
      "4003":"0399",
      "4004":"0303",
      "4005":"0309",
      "4006":"0313",
      "4007":"0313",
      "4008":"0313",
      "4009":"0313",
      "4010":"0399",
      "4011":"0399",
      "4012":"0399",
      "4013":"0311",
      "4014":"0301",
      "4015":"0317",
      "4016":"0303",
      "4017":"0307",
      "4018":"0399",
      "4019":"0303",
      "4099":"0399",
      "4101":"0599",
      "4102":"0109",
      "4103":"0509",
      "4104":"0509",
      "4105":"0599",
      "4106":"0107",
      "4199":"0599",
      "4201":"0601",
      "4202":"0613",
      "4203":"0613",
      "4204":"0603",
      "4205":"0603",
      "4206":"0613",
      "4207":"0601",
      "4208":"0619",
      "4299":"0699",
      "4301":"0903",
      "4302":"0903",
      "4303":"0903",
      "4399":"0903",
      "4401":"0903",
      "4402":"0999",
      "4403":"0999",
      "4404":"0903",
      "4405":"0903",
      "4406":"0903",
      "4407":"0901",
      "4408":"0901",
      "4409":"0905",
      "4410":"0903",
      "4499":"0903",
      "4501":"0903",
      "4502":"0799",
      "4503":"0509",
      "4504":"0613",
      "4505":"0903",
      "4506":"0699",
      "4507":"0903",
      "4508":"0799",
      "4509":"0509",
      "4510":"0613",
      "4511":"0903",
      "4512":"0699",
      "4513":"0903",
      "4514":"0799",
      "4515":"0509",
      "4516":"0613",
      "4517":"0699",
      "4518":"0903",
      "4519":"0203",
      "4599":"0999",
      "4601":"0299",
      "4602":"0201",
      "4603":"0201",
      "4604":"0201",
      "4605":"0203",
      "4606":"0201",
      "4607":"0201",
      "4608":"0203",
      "4609":"0203",
      "4610":"0913",
      "4611":"0201",
      "4612":"0201",
      "4613":"0201",
      "4699":"0299",
      "4701":"1007",
      "4702":"0903",
      "4703":"0915",
      "4704":"0915",
      "4705":"0915",
      "4799":"0903",
      "4801":"0909",
      "4802":"0909",
      "4803":"0909",
      "4804":"0909",
      "4805":"0909",
      "4806":"0909",
      "4807":"0909",
      "4899":"0909",
      "4901":"0101",
      "4902":"0101",
      "4903":"0101",
      "4904":"0101",
      "4905":"0101",
      "4999":"0101",
      "5001":"0917",
      "5002":"0917",
      "5003":"0917",
      "5004":"0917",
      "5005":"0917",
      "5099":"0917",
      "5101":"0103",
      "5102":"0103",
      "5103":"0103",
      "5104":"0103",
      "5105":"0103",
      "5106":"0103",
      "5107":"0103",
      "5108":"0103",
      "5109":"0103",
      "5110":"0103",
      "5199":"0103",
      "5201":"0907",
      "5202":"0907",
      "5203":"0907",
      "5204":"0907",
      "5205":"0907",
      "5299":"0907"
    };

    // given a set of apportionments, scale the weights to integers that sum to 100
    function finalise_apportionments(apps=[]) {
      const normalised_weights = normalise_weights(apps.map(v => v.weight));
      const consolidated = {};
      apps.forEach((app,i) => {
        const code = app.code;
        const weight = normalised_weights[i];
        consolidated[code] = (consolidated[code] ?? 0) + weight;
      });
      return Object.entries(consolidated).map(([code,weight]) => ({code,weight}));
    }

    // given a set of weights, scale them to integers that sum to 100
    function normalise_weights(weights=[]) {
      if (weights.length == 0) return [];
      let sum = weights.reduce((prev,curr) => prev + curr, 0);
      if (sum == 0) {
        weights = weights.map(v => 1);
        sum = weights.length;
      }
      const scale = 100 / sum;
      const final = weights.map(weight => weight * scale);
      for (let i=1; i<final.length; ++i) final[i] += final[i-1];
      for (let i=0; i<final.length; ++i) final[i] = Math.round(final[i]);
      for (let i=final.length-1; i>0; --i) final[i] = final[i] - final[i-1];
      return final;
    }

    function translate_fors(apps=[]) {
      let mapped2 = [];
      let mapped4 = [];

      // split and translate FoR codes into FoE codes
      for (let {code,weight} of apps) {
        let new_code = for_to_foe_map[code];
        if (new_code === undefined) { return; }
        if (new_code.length == 2) { mapped2.push({ code:new_code, weight:+weight }); }
        if (new_code.length == 4) { mapped4.push({ code:new_code, weight:+weight }); }
      }

      // if there are any 4-digit apportionments, these reform the 2-digit ones
      if (mapped4.length > 0) {
        mapped2 = mapped4.map(v => ({
          code   : v.code.substring(0,2),
          weight : v.weight
        }));
      }

      // re-weight, collapse and merge final apportionment
      return [
        ...finalise_apportionments(mapped2),
        ...finalise_apportionments(mapped4),
      ].map(v => ({
        vers   : 'foe',
        len    : v.code.length,
        code   : v.code,
        name   : null,
        weight : v.weight,
      }));
    }
    return translate_fors(apportionments);
  """;

  -- mutate the core_fors table so that it uses fields of education
  DELETE FROM \`${project}.${dataset}.core_fors${version}\` WHERE true;
  INSERT INTO \`${project}.${dataset}.core_fors${version}\` (vers,len,code,name) VALUES
  ('foe',2,'01','Natural and Physical Sciences'),
  ('foe',4,'0101','Mathematical Sciences'),
  ('foe',4,'0103','Physics and Astronomy'),
  ('foe',4,'0105','Chemical Sciences'),
  ('foe',4,'0107','Earth Sciences'),
  ('foe',4,'0109','Biological Sciences'),
  ('foe',4,'0199','Other Natural and Physical Sciences'),
  ('foe',2,'02','Information Technology'),
  ('foe',4,'0201','Computer Science'),
  ('foe',4,'0203','Information Systems'),
  ('foe',4,'0299','Other Information Technology'),
  ('foe',2,'03','Engineering and Related Technologies'),
  ('foe',4,'0301','Manufacturing Engineering and Technology'),
  ('foe',4,'0303','Process and Resources Engineering'),
  ('foe',4,'0305','Automotive Engineering and Technology'),
  ('foe',4,'0307','Mechanical and Industrial Engineering and Technology'),
  ('foe',4,'0309','Civil Engineering'),
  ('foe',4,'0311','Geomatic Engineering'),
  ('foe',4,'0313','Electrical and Electronic Engineering and Technology'),
  ('foe',4,'0315','Aerospace Engineering and Technology'),
  ('foe',4,'0317','Maritime Engineering and Technology'),
  ('foe',4,'0399','Other Engineering and Related Technologies'),
  ('foe',2,'04','Architecture and Building'),
  ('foe',4,'0401','Architecture and Urban Environment'),
  ('foe',4,'0403','Building'),
  ('foe',2,'05','Agriculture, Environmental and Related Studies'),
  ('foe',4,'0501','Agriculture'),
  ('foe',4,'0503','Horticulture and Viticulture'),
  ('foe',4,'0505','Forestry Studies'),
  ('foe',4,'0507','Fisheries Studies'),
  ('foe',4,'0509','Environmental Studies'),
  ('foe',4,'0599','Other Agriculture, Environmental and Related Studies'),
  ('foe',2,'06','Health'),
  ('foe',4,'0601','Medical Studies'),
  ('foe',4,'0603','Nursing'),
  ('foe',4,'0605','Pharmacy'),
  ('foe',4,'0607','Dental Studies'),
  ('foe',4,'0609','Optical Science'),
  ('foe',4,'0611','Veterinary Studies'),
  ('foe',4,'0613','Public Health'),
  ('foe',4,'0615','Radiography'),
  ('foe',4,'0617','Rehabilitation Therapies'),
  ('foe',4,'0619','Complementary Therapies'),
  ('foe',4,'0699','Other Health'),
  ('foe',2,'07','Education'),
  ('foe',4,'0701','Teacher Education'),
  ('foe',4,'0703','Curriculum and Education Studies'),
  ('foe',4,'0799','Other Education'),
  ('foe',2,'08','Management and Commerce'),
  ('foe',4,'0801','Accounting'),
  ('foe',4,'0803','Business and Management'),
  ('foe',4,'0805','Sales and Marketing'),
  ('foe',4,'0807','Tourism'),
  ('foe',4,'0809','Office Studies'),
  ('foe',4,'0811','Banking, Finance and Related Fields'),
  ('foe',4,'0899','Other Management and Commerce'),
  ('foe',2,'09','Society and Culture'),
  ('foe',4,'0901','Political Science and Policy Studies'),
  ('foe',4,'0903','Studies in Human Society'),
  ('foe',4,'0905','Human Welfare Studies and Services'),
  ('foe',4,'0907','Behavioural Science'),
  ('foe',4,'0909','Law'),
  ('foe',4,'0911','Justice and Law Enforcement'),
  ('foe',4,'0913','Librarianship, Information Management and Curatorial Studies'),
  ('foe',4,'0915','Language and Literature'),
  ('foe',4,'0917','Philosophy and Religious Studies'),
  ('foe',4,'0919','Economics and Econometrics'),
  ('foe',4,'0921','Sport and Recreation'),
  ('foe',4,'0999','Other Society and Culture'),
  ('foe',2,'10','Creative Arts'),
  ('foe',4,'1001','Performing Arts'),
  ('foe',4,'1003','Visual Arts and Crafts'),
  ('foe',4,'1005','Graphic and Design Studies'),
  ('foe',4,'1007','Communication and Media Studies'),
  ('foe',4,'1099','Other Creative Arts'),
  ('foe',2,'11','Food, Hospitality and Personal Services'),
  ('foe',4,'1101','Food and Hospitality'),
  ('foe',4,'1103','Personal Services'),
  ('foe',2,'12','Mixed Field Programmes'),
  ('foe',4,'1201','General Education Programmes'),
  ('foe',4,'1203','Social Skills Programmes'),
  ('foe',4,'1205','Employment Skills Programmes'),
  ('foe',4,'1299','Other Mixed Field Programmes'),
  ('foe',2,'99','Multidisciplinary');

  -- mutate the core_papers table
  CREATE TEMPORARY TABLE papers AS (SELECT * FROM \`${project}.${dataset}.core_papers${version}\`);
  DELETE FROM \`${project}.${dataset}.core_papers${version}\` WHERE TRUE;
  INSERT INTO \`${project}.${dataset}.core_papers${version}\` (doi,era_id,year_published,num_citations,is_oa,rors,heps,fors) 
  SELECT doi,era_id,year_published,num_citations,is_oa,rors,heps,fors_to_foes(fors) AS fors FROM papers;

  -- mutate the core_assignments table
  CREATE TEMPORARY TABLE assignments AS (
    SELECT year,journal,paper,cits,inst,is_hep,
      ARRAY_AGG(STRUCT(
        ''            AS vers,
        LENGTH(field) AS len,
        field         AS code,
        ''            AS name,
        frac          AS weight
      )) AS fors
    FROM \`${project}.${dataset}.core_assignments${version}\`
    GROUP BY year,journal,paper,cits,inst,is_hep
  );
  DELETE FROM \`${project}.${dataset}.core_assignments${version}\` WHERE TRUE;
  INSERT INTO \`${project}.${dataset}.core_assignments${version}\` (year,journal,paper,cits,inst,is_hep,field,field2,frac) 
  SELECT year,journal,paper,cits,inst,is_hep,
    F.code                AS field,
    SUBSTRING(F.code,0,2) AS field2,
    F.weight              AS frac
  FROM assignments LEFT JOIN UNNEST(fors_to_foes(fors)) AS F;

END;
`;
const compile_all = (args = {}) => [compile(args)];
module.exports = { compile, compile_all };
if (require.main === module) require("app").cli_compile(compile_all);
