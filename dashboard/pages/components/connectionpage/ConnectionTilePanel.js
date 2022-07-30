import React from "react";
import ConnectionTile from "./ConnectionTile";
import Grid from "@material-ui/core/Grid";

const TilePanel = ({ sections }) => {
  return (
    <div>
      <Grid container justifyContent="center" lg={12}>
        {sections.map((section, i) => {
          return (
            <Grid item xs={4} lg={2} key={`tile-grid-${i}`}>
              <ConnectionTile detail={section} key={`tile-${i}`} id={i} />
            </Grid>
          );
        })}
      </Grid>
    </div>
  );
};

export default TilePanel;
