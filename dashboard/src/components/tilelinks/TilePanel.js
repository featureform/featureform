import React from "react";
import Tile from "./Tile.js";
import Grid from "@material-ui/core/Grid";
import { makeStyles } from "@material-ui/core/styles";

const useStyles = makeStyles((theme) => ({
  root: {
    opacity: 0,
  },
}));
const TilePanel = ({ sections }) => {
  let classes = useStyles();
  return (
    <div>
      <Grid container justifyContent="center" lg={12}>
        {sections.map((section, i) => {
          return (
            <Grid item xs={4} lg={2} key={`tile-grid-${i}`}>
              <Tile detail={section} key={`tile-${i}`} id={i} />
            </Grid>
          );
        })}
      </Grid>
    </div>
  );
};

export default TilePanel;
