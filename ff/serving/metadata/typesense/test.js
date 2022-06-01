const Typesense = require("typesense");
const TypesenseClient = require("./typesenseClass.js");
const myTypesenseClient = new TypesenseClient("8108", "localhost", "xyz");
const myResults = myTypesenseClient.search(
  { q: "user", query_by: "Name" },
  "resource"
);
myResults.then((results) => {
  console.log(results.results());
});
