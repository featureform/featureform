export default class SearchEngine {
  scores = {
    name: 15,
    description: 5,
    tags: 5,
  };

  createFormattedStrings(matchPositions, item) {
    for (const k of Object.keys(matchPositions)) {
      if (matchPositions[k].size == 0) {
        continue;
      }
      let originalText;
      if (k === "name") {
        originalText = item.name;
      } else if (k === "description") {
        originalText = item.versions[item["default-variant"]].description;
      }

      let formattedString = "";
      let lastPos = 0;
      let posArray = Array.from(matchPositions[k]);
      posArray.sort((first, second) => {
        if (first < second) {
          return -1;
        }
        if (first > second) {
          return 1;
        }
        return 0;
      });
      for (let i = 0; i < posArray.length; i++) {
        const pos1 = posArray[i];
        i++;
        const pos2 = posArray[i];
        formattedString += originalText.substring(lastPos, pos1);
        formattedString += "<b>";
        formattedString += originalText.substring(pos1, pos2);
        formattedString += "</b>";
        lastPos = pos2;
      }
      formattedString += originalText.substring(lastPos, originalText.length);

      if (k === "name") {
        item["formattedName"] = formattedString;
      } else if (k === "description") {
        item["formattedDescription"] = formattedString;
      }
    }
  }

  sortValueList(list) {
    list.sort(function (first, second) {
      if (first[1] > second[1]) {
        return -1;
      }
      if (first[1] < second[1]) {
        return 1;
      }
      return 0;
    });
    let returnArray = [];
    list.forEach((item) => (item[1] ? returnArray.push(item[0]) : []));
    return returnArray;
  }

  sliceQuery(query) {
    let queryArray = query.trim().split(/[ ,]+/);
    let slices = [];
    for (let i = 0; i < queryArray.length; i++) {
      for (let j = i + 1; j < queryArray.length + 1; j++) {
        slices.push(queryArray.slice(i, j));
      }
    }
    return slices;
  }
  sortedResultsByRelevance(data, query) {
    let itemScores = [];
    let maxScore = 0;
    data.forEach((item) => {
      let score = 0;
      const itemData = {
        name: item.name.toLowerCase(),
        description: item.versions[item["default-variant"]].description
          ? item.versions[item["default-variant"]].description.toLowerCase()
          : "",
        tags: item.versions[item["default-variant"]].tags
          ? item.versions[item["default-variant"]].tags.join(" ").toLowerCase()
          : "",
      };

      const querySlices = this.sliceQuery(query);
      let matchPositions = {};
      Object.keys(this.scores).forEach((key) => {
        if (key !== "tags") {
          matchPositions[key] = new Set();
        }
      });
      querySlices.forEach((slice) => {
        const sliceLength = slice.length;
        const sliceString = slice.join(" ");

        for (const [k, v] of Object.entries(this.scores)) {
          const queryIndexInItem = itemData[k].indexOf(sliceString);
          if (queryIndexInItem > -1) {
            if (k !== "tags") {
              matchPositions[k].add(queryIndexInItem);
              matchPositions[k].add(queryIndexInItem + sliceString.length);
            }
            score += v * sliceLength * (Math.log(sliceString.length) + 1);
          }
        }
      });
      this.createFormattedStrings(matchPositions, item);
      maxScore = Math.max(maxScore, score);

      itemScores.push([item, score]);
    });

    let sortedItemList = this.sortValueList(itemScores);
    return [sortedItemList, maxScore];
  }

  filterSearch(query, unfilteredData) {
    const lowerCaseQuery = query.toLowerCase();
    let filteredData = {};
    filteredData["data"] = {};
    let keyOrder = [];
    Object.keys(unfilteredData).forEach((key, index) => {
      const sortedResults = this.sortedResultsByRelevance(
        unfilteredData[key],
        lowerCaseQuery
      );

      keyOrder.push([key, sortedResults[1]]);
      if (sortedResults[0].length > 0) {
        filteredData["data"][key] = sortedResults[0];
      }
    });
    let typeOrder = this.sortValueList(keyOrder);

    filteredData["typeOrder"] = typeOrder;
    return filteredData;
  }
}
