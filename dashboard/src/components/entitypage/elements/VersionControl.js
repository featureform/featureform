import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import InputLabel from "@material-ui/core/InputLabel";
import MenuItem from "@material-ui/core/MenuItem";
import FormHelperText from "@material-ui/core/FormHelperText";
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
    activeVersions: state.selectedVersion,
  };
}

const VersionControl = ({
  version,
  versions,
  handleVersionChange,
  entityPage,
  convertTimestampToDate,
  local,
}) => {
  const classes = useStyles();
  let createdDate;
  if (local) {
    createdDate =
      entityPage.resources["versions"][version]["metadata"]["created"];
  } else {
    createdDate = entityPage.resources["versions"][version]["created"];
  }

  const handleChange = (event) => {
    handleVersionChange(event);
  };

  return (
    <FormControl className={classes.formControl}>
      <InputLabel shrink id="demo-simple-select-placeholder-label-label">
        Variant
      </InputLabel>
      <Select
        labelId="demo-simple-select-placeholder-label-label"
        id="demo-simple-select-placeholder-label"
        value={version}
        onChange={handleChange}
        displayEmpty
        className={classes.selectEmpty}
      >
        {versions.map((version) => (
          <MenuItem key={version} value={version}>
            {version}
          </MenuItem>
        ))}
      </Select>
      <FormHelperText>
        <b>{`Created ${convertTimestampToDate(createdDate)}`}</b>
      </FormHelperText>
    </FormControl>
  );
};

export default connect(mapStateToProps)(VersionControl);
