import React from "react";
import { createStyles, alpha, makeStyles } from "@material-ui/core/styles";
import SearchIcon from "@material-ui/icons/Search";
import Paper from "@material-ui/core/Paper";
import Grid from "@material-ui/core/Grid";
import { useHistory } from "react-router-dom";

import InputBase from "@material-ui/core/InputBase";

import { generatePath } from "react-router";

const ENTER_KEY_CODE = 13;

const useStyles = makeStyles((theme) =>
  createStyles({
    search: {
      position: "relative",
      borderRadius: theme.shape.borderRadius,
      backgroundColor: alpha(theme.palette.common.white, 0.15),
      "&:hover": {
        backgroundColor: alpha(theme.palette.common.white, 0.25),
      },
    },
    searchIcon: {
      padding: theme.spacing(0, 2),
      height: "100%",
      position: "absolute",
      pointerEvents: "none",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
    },
    inputRoot: {
      color: "inherit",
    },
    inputInputHome: {
      padding: theme.spacing(1, 20, 1, 0),
      // vertical padding + font size from searchIcon
      paddingLeft: `calc(1em + ${theme.spacing(4)}px)`,
      transition: theme.transitions.create("width"),
    },
    inputInputTopBar: {
      padding: theme.spacing(1, 2, 1, 0),
      // vertical padding + font size from searchIcon
      paddingLeft: `calc(1em + ${theme.spacing(4)}px)`,
      transition: theme.transitions.create("width"),
    },
  })
);

const SearchBar = ({ input, setQuery, homePage }) => {
  const history = useHistory();
  const classes = useStyles();

  const [searchText, setSearchText] = React.useState("");

  function handleSearch(event) {
    //setQuery(searchText);
    let uri = generatePath("/search?q=:query", {
      query: searchText,
    });
    history.push(uri);
  }

  return (
    <div className={classes.search}>
      <Grid container item justifyContent="center" direction="row" lg={12}>
        <Paper>
          <div className={classes.searchIcon}>
            <SearchIcon />
          </div>
          <InputBase
            placeholder="Search..."
            onChange={(event) => {
              setSearchText(event.target.value);
            }}
            defaultValue={""}
            onKeyDown={(event) =>
              event.keyCode === ENTER_KEY_CODE && searchText.length > 0
                ? handleSearch(event)
                : ""
            }
            classes={{
              root: classes.inputRoot,
              input: homePage
                ? classes.inputInputHome
                : classes.inputInputTopBar,
            }}
            inputProps={{ "aria-label": "search " }}
          />
        </Paper>
      </Grid>
    </div>
  );
};

export default SearchBar;
