import Card from '@mui/material/Card';
import Typography from '@mui/material/Typography';
import { makeStyles } from '@mui/styles';
import React from 'react';

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: '100%',
    height: '100%',
  },
  card: {
    padding: theme.spacing(3),
    width: '100%',
    height: '100%',
    border: `2px solid black`,
    borderRadius: 16,
    background: '#FFFFFF',
    minWidth: '12em',
  },

  title: {
    fontSize: '2em',
    color: 'black',
  },
}));

const Tile = ({ detail }) => {
  const classes = useStyles(detail.current_status);

  return (
    <div className={classes.root}>
      <Card className={classes.card} elevation={0}>
        <div>
          <Typography className={classes.title} variant='h5'>
            <b>{detail.title}</b>
          </Typography>
          <Typography variant='body1'>{detail.current_status}</Typography>
        </div>
      </Card>
    </div>
  );
};

export default Tile;
