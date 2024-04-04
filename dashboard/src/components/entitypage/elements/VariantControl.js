import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import ArrowDropUpIcon from '@mui/icons-material/ArrowDropUp';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import {
  Alert,
  Box,
  Button,
  IconButton,
  InputAdornment,
  InputLabel,
  Popover,
  Slide,
  Snackbar,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import FormControl from '@mui/material/FormControl';
import { makeStyles } from '@mui/styles';
import React, { useEffect, useRef, useState } from 'react';
import { connect } from 'react-redux';
import VariantView from './VariantView';

const useStyles = makeStyles(() => ({
  formControl: {
    minWidth: 250,
  },
}));

function mapStateToProps(state) {
  return {
    entityPage: state.entityPage,
    activeVariants: state.selectedVariant,
  };
}

const VariantControl = ({
  variant,
  variantListProp = [],
  resources,
  handleVariantChange,
}) => {
  const classes = useStyles();
  const ref = useRef();

  const [open, setOpen] = useState(false);
  const [openVarDrop, setOpenVarDrop] = useState(false);
  const [variantList, setVariantList] = useState([]);
  const [filteredVarList, setFilteredVarList] = useState([]);
  const closeSnackBar = (_, reason) => {
    if (reason === 'clickaway') {
      return;
    }
    setOpen(false);
  };

  const copyToClipBoard = (variantName = '') => {
    if (variantName) {
      navigator.clipboard.writeText(variantName);
      setOpen(true);
    }
  };

  function transition(props) {
    return <Slide {...props} direction='right' />;
  }

  const createVariantViewItem = (propItem = {}, id = 0) => {
    let result = {
      id: id,
      name: '---',
      variant: propItem.variant,
      owner: propItem.owner,
      created: new Date(),
    };
    const propDate = new Date(propItem.created);
    if (!isNaN(propDate.getTime())) {
      //is valid
      result.created = propDate;
    } else {
      console.error(
        `error on (${propItem?.name} - ${propItem?.variant}) created field`,
        propItem?.created
      );
    }
    return result;
  };
  useEffect(() => {
    let tempList = [];
    if (variantListProp.length && resources?.variants) {
      tempList = variantListProp.map((vr, index) => {
        const foundRecord = resources.variants[vr];
        return createVariantViewItem(foundRecord, index);
      });
      tempList = tempList.sort((a, b) => b.created - a.created);
    }
    setVariantList(tempList);
  }, [variantListProp]);

  const handleSelect = (variantName = '') => {
    if (variantName) {
      handleVariantChange(variantName);
      setOpenVarDrop(false);
    }
  };

  const handleSearch = (searchText = '') => {
    let filteredList = variantList.filter((vr) => {
      return vr.name?.includes(searchText) || vr.variant?.includes(searchText);
    });
    setFilteredVarList(filteredList);
  };

  const handleClose = () => {
    setOpenVarDrop(false);
  };

  const handleOpen = () => {
    //reset the filtered list
    setFilteredVarList(variantList);
    setOpenVarDrop(true);
  };

  return (
    <>
      <Snackbar
        open={open}
        autoHideDuration={1250}
        onClose={closeSnackBar}
        TransitionComponent={transition}
      >
        <Alert severity='success' onClose={closeSnackBar}>
          <Typography>Copied to clipboard!</Typography>
        </Alert>
      </Snackbar>

      <Box style={{ minWidth: 325 }}>
        <FormControl className={classes.formControl}>
          <InputLabel shrink id='variantControlId'>
            Variant
          </InputLabel>
          <TextField
            value={variant}
            label='Variant'
            onChange={handleSelect}
            onClick={handleOpen}
            type='button'
            ref={ref}
            InputProps={{
              endAdornment: (
                <InputAdornment position='end'>
                  <IconButton>
                    {openVarDrop ? <ArrowDropUpIcon /> : <ArrowDropDownIcon />}
                  </IconButton>
                </InputAdornment>
              ),
            }}
            inputProps={{
              'aria-label': 'searchVariant',
              'data-testid': 'variantControlSelectId',
            }}
          ></TextField>
        </FormControl>
        <Tooltip title='Copy to Clipboard'>
          <Button
            role='none'
            onClick={() => copyToClipBoard(variant)}
            style={{ marginTop: 10 }}
          >
            <ContentCopyIcon fontSize='medium' />
          </Button>
        </Tooltip>
        <Popover
          style={{ marginTop: '0.25em' }}
          open={openVarDrop}
          anchorReference='anchorEl'
          onClose={handleClose}
          anchorEl={ref?.current}
          anchorOrigin={{
            vertical: 'bottom',
            horizontal: 'left',
          }}
        >
          <VariantView
            variantList={filteredVarList}
            handleClose={handleClose}
            handleSelect={handleSelect}
            handleSearch={handleSearch}
          />
        </Popover>
      </Box>
    </>
  );
};

export default connect(mapStateToProps)(VariantControl);
