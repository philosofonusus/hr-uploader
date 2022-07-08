const fs = require("fs");
const path = require("path");
const csv = require("csvtojson");
const extract = require("extract-zip");
const rimraf = require("rimraf");
const jq = require("node-jq");
const {once} = require('events');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

const readDir = path.join(__dirname);

const jqFilter = "{full_name,first_name,middle_initial,middle_name,last_name,gender,birth_year,birth_date,linkedin_url,linkedin_username,linkedin_id,facebook_url,facebook_username,facebook_id,twitter_url,twitter_username,github_url,github_username,work_email,mobile_phone,industry,job_title,job_title_role,job_title_sub_role,job_title_levels,job_company_id,job_company_name,job_company_website,job_company_size,job_company_founded,job_company_industry,job_company_linkedin_url,job_company_linkedin_id,job_company_facebook_url,job_company_twitter_url,job_company_location_name,job_company_location_locality,job_company_location_metro,job_company_location_region,job_company_location_geo,job_company_location_street_address,job_company_location_address_line_2,job_company_location_postal_code,job_company_location_country,job_company_location_continent,job_last_updated,job_start_date,job_summary,location_name,location_locality,location_metro,location_region,location_country,location_continent,location_street_address,location_address_line_2,location_postal_code,location_geo,location_last_updated,linkedin_connections,inferred_salary,inferred_years_experience,summary,phone_numbers,emails,interests,skills,location_names,regions,countries,street_addresses,experience,education,profiles,certifications,languages}";

const entries = fs
  .readdirSync(readDir)
  .filter((file) => file.split(".")[1] === "zip")
  .map((file) => file.split(".")[0]);

entries.forEach(async (entry) => {
  try {
  console.log(entry);
  await extract(
    `${readDir}/${entry}.zip`,
    { dir: `${readDir}/${entry}` },
    (err) => {
      if (err) {
        return console.log(err);
      }
      console.log("done");
    }
  );

  const csvFilePath = path.join(readDir, entry, "full", `${entry}.csv`);
  const readStream = fs.createReadStream(csvFilePath);

  const jsonFilePath = path.join(readDir, entry, "full", `${entry}.json`);
  const writeSteam = fs.createWriteStream(jsonFilePath);

  const stream = readStream.pipe(csv()).pipe(writeSteam);

  await once(stream, 'finish');

  console.log(jsonFilePath);

  const filteredPath = path.join(readDir, entry, "full", `${entry}_filtered.json`)
  await exec(`./jq '${jqFilter}' ${jsonFilePath} > ${filteredPath}`)

  console.log("filtered")

  const {stdout, stderr} = await exec(`mongoimport --host 45.82.64.68 --port 27017 --db admin --collection workers --type json --file ${filteredPath} --batchSize 1`);

  console.log(stdout, stderr);

  await rimraf(path.join(readDir, entry),(err) => console.log(err) )

  console.log('done', entry)
  } catch (error) {
    console.log(error)
  }
});
