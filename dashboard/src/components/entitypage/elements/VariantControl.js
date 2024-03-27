import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import {
  Alert,
  Box,
  Button,
  InputLabel,
  Slide,
  Snackbar,
  Tooltip,
  Typography,
} from '@mui/material';
import FormControl from '@mui/material/FormControl';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import { makeStyles } from '@mui/styles';
import React from 'react';
import { connect } from 'react-redux';

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

      <Box style={{ minWidth: 325 }}>
        <FormControl className={classes.formControl}>
          <InputLabel shrink={true} id='variantControlId'>
            Variant
          </InputLabel>
          <Select
            value={variant}
            InputLabelProps={{ shrink: true }}
            labelId='variantControlId'
            label='Variant'
            data-testid={'variantControlSelectId'}
            onChange={handleChange}
            notched
            displayEmpty
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
          <Button
            role='none'
            onClick={() => copyToClipBoard(variant)}
            style={{ marginTop: 10 }}
          >
            <ContentCopyIcon fontSize='medium' />
          </Button>
        </Tooltip>
      </Box>
    </>
  );
};

export default connect(mapStateToProps)(VariantControl);
