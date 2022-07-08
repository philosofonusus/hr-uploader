const fs = require("fs");
const JSONStream = require("JSONStream");
const es = require("event-stream");
const jsonminify = require("jsonminify");

let id = -1;

fs.createReadStream(__dirname + "/tokelau/full/tokelau_filtered.json")
  .pipe(JSONStream.parse())
  .pipe(JSONStream.stringify())
  .pipe(
    es.mapSync(function (data) {
      fs.appendFileSync(
        "bulk.json",
        `\n{"index":{"_index":"users","_id":${(id += 1)}}}`
      );
      fs.appendFileSync("bulk.json", "\n" + jsonminify(data));
      return data;
    })
  );

//curl -s -H "Content-Type: application/x-ndjson" -XPOST http://'elastic:hJnRI_tCNoEwD67kCBuo'@localhost:9200/_bulk?pretty --data-binary @bulk.json
