import React from "react";
import { useLocation } from "react-router-dom";
import { makeStyles } from "@material-ui/core/styles";
import { Link } from "react-router-dom";
import Breadcrumbs from "@material-ui/core/Breadcrumbs";
import Icon from "@material-ui/core/Icon";

const useStyles = makeStyles((theme) => ({
  root: {
    margin: 10,
  },
}));

const BreadCrumbs = () => {
  const classes = useStyles();
  const path = useLocation().pathname.split("/");
  while (path.length > 0 && path[0].length === 0) {
    path.shift();
  }

  const capitalize = (word) => {
    return word[0].toUpperCase() + word.slice(1).toLowerCase();
  };

  const pathBuilder = (accumulator, currentValue) =>
    accumulator + "/" + currentValue;
  return (
    <div className={classes.root}>
      {path.length > 0 ? (
        <Breadcrumbs
          aria-label="breadcrumb"
          //separator={<Icon>{"navigatenext"}</Icon>}
        >
          <Link to="/">Home</Link>
          {path.map((ent, i) => (
            <Link
              key={`link-${i}`}
              to={"/" + path.slice(0, i + 1).reduce(pathBuilder)}
            >
              <b>{capitalize(ent)}</b>
            </Link>
          ))}
        </Breadcrumbs>
      ) : (
        <div></div>
      )}
    </div>
  );
};

export default BreadCrumbs;
