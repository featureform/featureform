import React from "react";
import Tile from "./Tile.js";
import Grid from "@material-ui/core/Grid";

const TilePanel = ({ sections }) => {
  return (
    <div>
      <Grid container justifyContent="center" lg={12}>
        {sections.map((section, i) => {
          return (
            <Grid item xs={12} md={6} lg={4} key={`tile-grid-${i}`}>
              <Tile detail={section} key={`tile-${i}`} id={i} />
            </Grid>
          );
        })}
      </Grid>
    </div>
  );
};

export default TilePanel;
