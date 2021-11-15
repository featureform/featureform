import React from "react";
import Icon from "@material-ui/core/Icon";
import { makeStyles } from "@material-ui/core/styles";
import { useHistory } from "react-router-dom";
import ButtonBase from "@material-ui/core/ButtonBase";
import Typography from "@material-ui/core/Typography";

const useStyles = makeStyles((theme, id) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
    height: "100%",
  },
  card: {
    padding: theme.spacing(3),
    width: "100%",
    height: "100%",
    border: (id) => `2px solid ${theme.palette.ordinalColors[id]}`,
    borderRadius: 16,
    background: "white",
    minWidth: "12em",
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
    width: "100%",
    height: "100%",
  },
  buttonDisabled: {
    padding: theme.spacing(3),
    opacity: 0.3,
    width: "100%",
    height: "100%",
    border: (id) => `2px solid ${theme.palette.ordinalColors[id]}`,
    borderRadius: 16,
    background: "white",
    minWidth: "12em",
  },
  icon: {
    marginBottom: theme.spacing(1),
    color: (id) => theme.palette.ordinalColors[id],
    fontSize: "2em",
  },
  title: {
    fontSize: "2em",
    color: (id) => theme.palette.ordinalColors[id],
  },
}));

const Tile = ({ detail, id }) => {
  let history = useHistory();
  const disabled = detail.disabled;
  const classes = useStyles(id);

  let buttonClass = classes.card;

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
        <div>
          <Icon color="green" className={classes.icon}>
            {detail.icon}
          </Icon>

          <Typography className={classes.title} variant="h5">
            <b>{detail.title}</b>
          </Typography>
        </div>
      </ButtonBase>
    </div>
  );
};

export default Tile;
