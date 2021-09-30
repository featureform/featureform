import React from "react";
import Icon from "@material-ui/core/Icon";
import Paper from "@material-ui/core/Paper";
import { makeStyles } from "@material-ui/core/styles";
import { useHistory } from "react-router-dom";
import { CardContent } from "@material-ui/core";
import ButtonBase from "@material-ui/core/ButtonBase";
import Typography from "@material-ui/core/Typography";
import Grid from "@material-ui/core/Grid";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(3),
    width: "100%",
    height: "100%",
  },
  card: {
    width: "100%",
    height: "100%",
  },

  media: {
    height: 200,
  },
  text: {
    flex: 1,
  },
  cardTitle: {
    display: "flex",
  },
  button: {
    opacity: 1,
    width: "100%",
    height: "100%",
  },
  buttonDisabled: {
    opacity: 0.3,
    width: "100%",
    height: "100%",
  },
}));

const Tile = ({ detail }) => {
  let history = useHistory();
  const disabled = detail.disabled;
  const classes = useStyles();

  let buttonClass = classes.button;

  if (disabled) {
    buttonClass = classes.buttonDisabled;
  }

  const handleClick = (event) => {
    history.push(detail.path);
  };

  return (
    <div className={classes.root}>
      <ButtonBase
        className={buttonClass}
        onClick={handleClick}
        disabled={disabled}
      >
        <Paper className={classes.card}>
          <CardContent>
            <Grid
              container
              spacing={2}
              direction="row"
              justifyContent="flex-start"
              alignItems="flex-start"
              lg={12}
            >
              <Grid item xs={1}>
                {" "}
                <Icon>{detail.icon}</Icon>
              </Grid>
            </Grid>
            <Typography variant="h5">{detail.title}</Typography>
            <Typography variant="body1">{detail.description}</Typography>
          </CardContent>
        </Paper>
      </ButtonBase>
    </div>
  );
};

export default Tile;
