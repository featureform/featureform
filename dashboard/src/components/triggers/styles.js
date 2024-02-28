import { makeStyles } from '@mui/styles';

const useStyles = makeStyles(() => ({
  inputRow: {
    paddingBottom: 20,
    '& button': {
      height: 45,
    },
  },
  triggerBox: {
    height: 750,
    width: 700,
  },
}));

export { useStyles };
