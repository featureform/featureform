import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import InputLabel from "@material-ui/core/InputLabel";
import MenuItem from "@material-ui/core/MenuItem";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";
import { connect } from "react-redux";

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
  variants,
  handleVariantChange,
  entityPage,
  convertTimestampToDate,
  local,
}) => {
  const classes = useStyles();

  const handleChange = (event) => {
    handleVariantChange(event);
  };

  return (
    <FormControl className={classes.formControl}>
      <InputLabel shrink id="demo-simple-select-placeholder-label-label">
        Variant
      </InputLabel>
      <Select
        labelId="demo-simple-select-placeholder-label-label"
        id="demo-simple-select-placeholder-label"
        value={variant}
        onChange={handleChange}
        displayEmpty
        className={classes.selectEmpty}
      >
        {variants.map((variant) => (
          <MenuItem key={variant} value={variant}>
            {variant}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

export default connect(mapStateToProps)(VariantControl);
