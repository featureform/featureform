import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import Chip from '@material-ui/core/Chip';
import Container from '@material-ui/core/Container';

const useStyles = makeStyles((theme) => ({
  formControl: {
    margin: theme.spacing(1),
    minWidth: 120,
  },
  selectEmpty: {
    marginTop: theme.spacing(2),
  },
  attributeContainer: {
    padding: theme.spacing(2),
    borderRadius: '16px',
    border: `1px solid ${theme.palette.border.main}`,
  },
  chip: {
    margin: theme.spacing(0.5),
  },
}));

/**
 *
 * @param {attributes} param0 - ([]string) A list attributes string (e.g. resource tags)
 * @param {title} param1 - (string) The attributes' title (e.g. "Tags")
 * @returns {Container}
 */
const AttributeBox = ({ attributes, title }) => {
  const classes = useStyles();

  return (
    <Container className={classes.attributeContainer}>
      <Typography variant='h6' component='h5' gutterBottom>
        {title}
      </Typography>
      {attributes ? (
        attributes.map((attr) => (
          <Chip
            label={attr}
            key={attr}
            className={classes.chip}
            variant='outlined'
          />
        ))
      ) : (
        <div></div>
      )}
    </Container>
  );
};

export default AttributeBox;
