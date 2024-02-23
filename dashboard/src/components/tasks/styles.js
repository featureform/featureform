import { makeStyles } from '@mui/styles';

const useStyles = makeStyles(() => ({
  intputRow: {
    paddingBottom: 30,
    '& button': {
      height: 56,
    },
  },
  activeButton: {
    color: '#FFFFFF',
    backgroundColor: '#7A14E5',
    '&:hover': {
      backgroundColor: '#7A14E5',
    },
  },
  activeChip: {
    color: '#000000',
    background: '#FFFFFF',
  },
  inactiveChip: {
    color: '#FFFFFF',
    background: '#7A14E5',
  },
  inactiveButton: {
    color: '#000000',
    background: '#FFFFFF',
  },
  buttonText: {
    textTransform: 'none',
    paddingRight: 10,
  },
  filterInput: {
    minWidth: 250,
  },
  taskCardBox: {
    height: 750,
    width: 700,
  },
}));

export { useStyles };
