import React from "react";
import { createStyles, alpha, makeStyles } from "@material-ui/core/styles";
import SearchIcon from "@material-ui/icons/Search";
import Grid from "@material-ui/core/Grid";
import { useHistory } from "react-router-dom";
import Container from "@material-ui/core/Container";
import Chip from "@material-ui/core/Chip";
import Icon from "@material-ui/core/Icon";

const links = [
  {
    title: "Wine Quality Dataset",
    icon: "storage",
    link: "/training-datasets/Wine%20Quality%20Dataset",
  },
  { title: "Fixed Acidity", link: "/features/fixed_acidity" },
  { title: "Wine spoiled", link: "/labels/Wine%20spoiled" },
  { title: "Wine id", link: "/entities/wine_id" },
];

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(3),
    alignItems: "center",
    justifyContent: "center",
  },
  chip: {
    padding: theme.spacing(1),
    margin: theme.spacing(1),
    paddingBottom: theme.spacing(1),
    paddingTop: theme.spacing(1),
    "& .MuiChip-labelSmall": {
      paddingTop: "0.2em",
    },
  },
  links: {
    padding: theme.spacing(0),
    justifyContent: "space-around",
    display: "flex",
    margin: "auto",
    width: "1px",
  },
  chipbox: {
    padding: theme.spacing(0),
  },
}));

const QuickLinks = ({ sections }) => {
  const history = useHistory();
  const classes = useStyles();

  function handleClick(item) {
    history.push(item.link);
  }

  return (
    <div className={classes.links}>
      {links.map((link) => (
        <div className={classes.chipbox}>
          <Chip
            size="small"
            label={link.title}
            key={link.link}
            className={classes.chip}
            onClick={() => handleClick(link)}
            variant="outlined"
          />
        </div>
      ))}
    </div>
  );
};

const mapStateToProps = (state) => ({
  sections: state.homePageSections,
});

export default QuickLinks;
