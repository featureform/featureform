import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import {
  Alert,
  IconButton,
  Slide,
  Snackbar,
  Tooltip,
  Typography,
} from '@mui/material';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import { makeStyles } from '@mui/styles';
import React from 'react';
import { connect } from 'react-redux';

const useStyles = makeStyles((theme) => ({
  formControl: {
    margin: theme.spacing(1),
    minWidth: 250,
  },
  selectEmpty: {
    marginTop: theme.spacing(2),
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

  const [open, setOpen] = React.useState(false);
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

  const handleChange = (event) => {
    handleVariantChange(event.target.value);
  };

  let variantList = [];

  if (variantListProp.length && resources?.variants) {
    try {
      variantList = variantListProp.map((vr) => {
        return {
          variantName: vr,
          created: new Date(resources.variants[vr].created),
        };
      });

      variantList = variantList.sort((a, b) => b.created - a.created);
    } catch (e) {
      console.error('Error ordering the variants:', e);
      variantList = variantListProp.map((vr) => {
        return { variantName: vr, created: null };
      });
    }
  }

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

      <FormControl className={classes.formControl}>
        <InputLabel shrink id='demo-simple-select-placeholder-label-label'>
          Variant
        </InputLabel>
        <Select
          labelId='demo-simple-select-placeholder-label-label'
          id='demo-simple-select-placeholder-label'
          data-testid={'variantControlSelectId'}
          value={variant}
          onChange={handleChange}
          displayEmpty
          className={classes.selectEmpty}
        >
          {variantList.map((vr, index) => (
            <MenuItem
              data-testid={`${index}_${vr?.variantName}`}
              key={vr?.variantName}
              value={vr?.variantName}
            >
              {vr?.variantName}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <Tooltip title='Copy to Clipboard'>
        <IconButton onClick={() => copyToClipBoard(variant)} fontSize={11.5}>
          <ContentCopyIcon />
        </IconButton>
      </Tooltip>
    </>
  );
};

export default connect(mapStateToProps)(VariantControl);
