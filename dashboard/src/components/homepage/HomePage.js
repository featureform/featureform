import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import { connect } from "react-redux";
import SearchBar from "../search/SearchBar";
import QuickLinks from "./QuickLinks";
import TilePanel from "../tilelinks/TilePanel";
import Typography from "@material-ui/core/Typography";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(0),
    alignItems: "center",
    justifyContent: "center",
  },
  search: {
    padding: theme.spacing(2),
    width: "40%",
    margin: "auto",
  },
  searchTitle: {
    width: "100%",
    display: "flex",
    flexWrap: "wrap",
    justifyContent: "center",
    padding: theme.spacing(0.5),
    marginBottom: theme.spacing(2),
  },
  clickPills: {
    width: "60%",
    margin: "auto",
  },
  title: {
    marginBottom: theme.spacing(1),
  },
  sections: {
    padding: theme.spacing(3),
  },
  tilePanel: {
    padding: theme.spacing(4),
  },
  tileBackground: {
    opacity: 0,
  },
}));

const HomePage = ({ sections }) => {
  let classes = useStyles();

  return (
    <div className={classes.root}>
      <div className={classes.search}>
        <div className={classes.searchTitle}></div>
        <SearchBar homePage={true} />
      </div>
      <div className={classes.clickPills}>
        <QuickLinks />
      </div>
      <div className={classes.sections}>
        <div className={classes.tilePanel}>
          <div className={classes.title}>
            <Typography variant="h5"></Typography>
          </div>
          <TilePanel sections={sections["features"]} />
        </div>
      </div>
    </div>
  );
};

const mapStateToProps = (state) => ({
  sections: state.homePageSections,
});

export default connect(mapStateToProps)(HomePage);
