import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import Chip from "@material-ui/core/Chip";

const links = [
  {
    title: "User Transaction Count",
    link: "/features/User%20Transaction%20Count",
  },
  {
    title: "Fraud Detection Training Set",
    link: "/training-sets/Fraud%20Detection%20Training%20Set",
  },
  { title: "Customer", link: "/entities/Customer" },
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

const QuickLinks = () => {
  const history = useHistory();
  const classes = useStyles();

  function handleClick(item) {
    history.push(item.link);
  }

  return (
    <div className={classes.links}>
      {links.map((link) => (
        <div className={classes.chipbox} key={link.title}>
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

export default QuickLinks;
