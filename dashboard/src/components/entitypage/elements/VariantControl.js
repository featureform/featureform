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
    minWidth: 120,
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

  const handleChange = (event) => {
    handleVariantChange(event);
  };

  let variantList = [];

  try {
    if (variantListProp.length && resources?.variants) {
      variantList = variantListProp.map((vr) => {
        return {
          name: vr,
          created: new Date(resources.variants[vr].created),
        };
      });

      variantList = variantList.sort((a, b) => b.created - a.created);
    }
  } catch (e) {
    console.error('Error ordering the variants:', e);
  }

  return (
    <FormControl className={classes.formControl}>
      <InputLabel shrink id='demo-simple-select-placeholder-label-label'>
        Variant
      </InputLabel>
      <Select
        labelId='demo-simple-select-placeholder-label-label'
        id='demo-simple-select-placeholder-label'
        value={variant}
        onChange={handleChange}
        displayEmpty
        className={classes.selectEmpty}
      >
        {variantList.map((variant, index) => (
          <MenuItem
            data-testid={index}
            key={variant?.name}
            value={variant?.name}
          >
            {variant?.name}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

export default connect(mapStateToProps)(VariantControl);
