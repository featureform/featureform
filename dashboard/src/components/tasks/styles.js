import { makeStyles } from '@mui/styles';

const useStyles = makeStyles(() => ({
  inputRow: {
    paddingBottom: 20,
    '& button': {
      height: 40,
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
    height: 20,
    width: 45,
    fontSize: 11,
  },
  inactiveChip: {
    color: '#FFFFFF',
    background: '#7A14E5',
    height: 20,
    width: 45,
    fontSize: 11,
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
    minWidth: 225,
    height: 40,
  },
  taskCardBox: {
    height: 750,
    width: 700,
  },
}));

export { useStyles };
