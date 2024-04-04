import { makeStyles } from '@mui/styles';

const useStyles = makeStyles(() => ({
  inputRow: {
    paddingBottom: 10,
    '& button': {
      height: 40,
    },
  },
  activeButton: {
    color: '#000000',
    fontWeight: 'bold',
  },
  activeChip: {
    color: '#FFFFFF',
    background: '#7A14E5',
    height: 20,
    width: 45,
    fontSize: 11,
  },
  inactiveChip: {
    color: '#FFFFFF',
    background: '#BFBFBF',
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
