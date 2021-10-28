import React from "react";
import { useLocation } from "react-router-dom";
import { makeStyles } from "@material-ui/core/styles";
import { Link } from "react-router-dom";
import Breadcrumbs from "@material-ui/core/Breadcrumbs";
import NavigateNextIcon from "@mui/icons-material/NavigateNext";

const useStyles = makeStyles((theme) => ({
  root: {
    margin: 5,
  },
  ol: {
    alignItems: "inherit",
  },
  breadcrumbs: {
    fontSize: 18,
  },
  separator: {
    marginLeft: "2px",
    marginRight: "2px",
    alignItems: "auto",
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
          className={classes.breadcrumbs}
          style={{ margin: "3px" }}
          aria-label="breadcrumb"
          separator={<NavigateNextIcon fontSize="medium" />}
          classes={{
            separator: classes.separator,
            ol: classes.ol,
          }}
        >
          <Link to="/">Home</Link>
          {path.map((ent, i) => (
            <Link
              key={`link-${i}`}
              to={"/" + path.slice(0, i + 1).reduce(pathBuilder)}
            >
              {i === path.length - 1 ? (
                <b>{capitalize(ent)}</b>
              ) : (
                capitalize(ent)
              )}
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
