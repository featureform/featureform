import React from "react";
import ConnectionTilePanel from "./ConnectionTilePanel";

export const ConnectionPageView = ({ statuses }) => {
  let features = [];
  Object.entries(statuses).forEach((entry) => {
    features.push({ title: entry[0], current_status: entry[1] });
  });

  return <ConnectionTilePanel sections={features} />;
};

export default ConnectionPageView;
