import React from "react";
import { createStyles, alpha, makeStyles } from "@material-ui/core/styles";
import SearchIcon from "@material-ui/icons/Search";
import Grid from "@material-ui/core/Grid";
import { useHistory } from "react-router-dom";
import Container from "@material-ui/core/Container";

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
      marginLeft: 0,
      width: "100%",
      [theme.breakpoints.up("sm")]: {
        marginLeft: theme.spacing(1),
        width: "auto",
      },
    },
    searchIcon: {
      padding: theme.spacing(0, 0),
      height: "100%",
      position: "absolute",
      pointerEvents: "none",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
    },
    border: {
      border: `2px solid #CDD1D9`,
      borderRadius: 16,
      //width: "40%",
      "&:hover": {
        border: `2px solid black`,
      },
    },
    inputRoot: {
      borderRadius: 16,
      color: "inherit",
      background: "transparent",
      boxShadow: "none",
      //paddingLeft: `calc(1em + ${theme.spacing(4)})`,
      transition: theme.transitions.create("width"),
      width: "100%",
    },
    inputInputHome: {
      //padding: theme.spacing(1, 20, 1, 0),
      // vertical padding + font size from searchIcon
      //width: "40%",
      paddingLeft: theme.spacing(4),
      transition: theme.transitions.create("width"),
      background: "transparent",
      boxShadow: "none",
    },
    inputTopBar: {
      padding: theme.spacing(1, 0, 0, 0),
      width: "100%",
      // vertical padding + font size from searchIcon
      paddingLeft: theme.spacing(4),
      transition: theme.transitions.create("width"),
      background: "transparent",
      boxShadow: "none",
      color: "black",
      alignSelf: "center",
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
      <Grid container item justifyContent="center" direction="row">
        <Container className={classes.border}>
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
              input: homePage ? classes.inputInputHome : classes.inputTopBar,
            }}
            inputProps={{ "aria-label": "search " }}
          />
        </Container>
      </Grid>
    </div>
  );
};

export default SearchBar;
