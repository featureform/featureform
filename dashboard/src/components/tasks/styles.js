import { makeStyles } from '@mui/styles';

const useStyles = makeStyles(() => ({
  intputRow: {
    paddingBottom: 30,
    '& button': {
      height: 56,
    },
  },
  activeButton: {
    color: 'white',
    backgroundColor: '#FC195C',
    '&:hover': {
      backgroundColor: '#FC195C',
    },
  },
  activeChip: {
    color: 'black',
    background: 'white',
  },
  inactiveChip: {
    color: 'white',
    background: '#FC195C',
  },
  inactiveButton: {
    color: 'black',
    background: 'white',
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
