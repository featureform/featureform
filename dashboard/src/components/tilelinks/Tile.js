import React from "react";
import Icon from "@material-ui/core/Icon";
import Paper from "@material-ui/core/Paper";
import { makeStyles } from "@material-ui/core/styles";
import { useHistory } from "react-router-dom";
import { CardContent } from "@material-ui/core";
import ButtonBase from "@material-ui/core/ButtonBase";
import Typography from "@material-ui/core/Typography";
import Container from "@material-ui/core/Container";
import Grid from "@material-ui/core/Grid";

const colors = {
  0: "#3D0A73",
  1: "#F7195C",
  2: "#AF72EF",
  3: "#F9477D",
  4: "#061B3F",
  5: "#7C0D2E",
};

const useStyles = makeStyles((theme, id) => ({
  root: {
    padding: theme.spacing(3),
    width: "100%",
    height: "100%",
  },
  card: {
    padding: theme.spacing(3),
    width: "100%",
    height: "100%",
    border: (id) => `2px solid ${colors[id]}`,
    borderRadius: 16,
    background: "#FFFFFF",
    minWidth: "200px",
    //borderRadius: 8
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
  icon: {
    marginBottom: theme.spacing(1),
    marginTop: theme.spacing(2),
    color: (id) => colors[id],
  },
  title: {
    marginBottom: theme.spacing(2),
    color: (id) => colors[id],
  },
}));

const Tile = ({ detail, id }) => {
  console.log(id);
  let history = useHistory();
  const disabled = detail.disabled;
  const classes = useStyles(id);

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
        <Container className={classes.card}>
          <Icon color="green" className={classes.icon}>
            {detail.icon}
          </Icon>

          <Typography className={classes.title} variant="h5">
            {detail.title}
          </Typography>
        </Container>
      </ButtonBase>
    </div>
  );
};

export default Tile;
