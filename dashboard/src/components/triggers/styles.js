import { makeStyles } from '@mui/styles';

const useStyles = makeStyles(() => ({
  inputRow: {
    paddingBottom: 10,
    display: 'flex',
    justifyContent: 'right',
    '& button': {
      height: 40,
    },
  },
  triggerBox: {
    height: 750,
    width: 700,
  },
}));

export { useStyles };
