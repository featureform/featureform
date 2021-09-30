import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Paper from "@material-ui/core/Paper";
import Chip from "@material-ui/core/Chip";

const useStyles = makeStyles((theme) => ({
  formControl: {
    margin: theme.spacing(1),
    minWidth: 120,
  },
  selectEmpty: {
    marginTop: theme.spacing(2),
  },
  tags: {
    padding: theme.spacing(2),
  },
  chip: {
    margin: theme.spacing(0.5),
  },
}));

const TagBox = ({ tags }) => {
  const classes = useStyles();

  return (
    <Paper className={classes.tags}>
      <Typography variant="h6" component="h5" gutterBottom>
        Tags
      </Typography>
      {tags ? (
        tags.map((tag) => (
          <Chip
            label={tag}
            key={tag}
            className={classes.chip}
            variant="outlined"
          />
        ))
      ) : (
        <div></div>
      )}
    </Paper>
  );
};

export default TagBox;
