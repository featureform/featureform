import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import Card from "@material-ui/core/Card";
import Typography from "@material-ui/core/Typography";

const useStyles = makeStyles((theme, current_status) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
    height: "100%",
  },
  card: {
    padding: theme.spacing(3),
    width: "100%",
    height: "100%",
    border: `2px solid black`,
    borderRadius: 16,
    background: "white",
    minWidth: "12em",
  },

  title: {
    fontSize: "2em",
    color: "black",
  },
}));

const Tile = ({ detail }) => {
  const classes = useStyles(detail.current_status);

  return (
    <div className={classes.root}>
      <Card className={classes.card} elevation={0}>
        <div>
          <Typography className={classes.title} variant="h5">
            <b>{detail.title}</b>
          </Typography>
          <Typography variant="body1">{detail.current_status}</Typography>
        </div>
      </Card>
    </div>
  );
};

export default Tile;
