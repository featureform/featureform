const Typesense = require('typesense')
const TypesenseClient = require("./search.js");
const myTypesenseClient = new TypesenseClient("8108", "localhost", "xyz");
const myResults = myTypesenseClient.search(
{ q: "smple", query_by: "name" },
"featuredata"
);
myResults.then((results) => {
console.log(results);
});