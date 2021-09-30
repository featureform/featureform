import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import Grid from "@material-ui/core/Grid";
import AppBar from "@material-ui/core/AppBar";

import Box from "@material-ui/core/Box";
import Tabs from "@material-ui/core/Tabs";
import Tab from "@material-ui/core/Tab";
import PropTypes from "prop-types";
import Typography from "@material-ui/core/Typography";
import FeatureDistribution from "../../graphs/FeatureDistribution";
import FeatureSetList from "../../lists/FeatureSetList";

function TabPanel(props) {
  const { children, value, index } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
    >
      {value === index && (
        <Box p={3}>
          <Typography>{children}</Typography>
        </Box>
      )}
    </div>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.any.isRequired,
  value: PropTypes.any.isRequired,
};

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    "aria-controls": `simple-tabpanel-${index}`,
  };
}

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
    backgroundColor: theme.palette.background.paper,
  },
  tabs: {
    borderRight: `1px solid ${theme.palette.divider}`,
  },
  detail: {
    margin: theme.spacing(1),
  },
}));

const FeatureDataView = ({ data }) => {
  const classes = useStyles();

  const [value, setValue] = React.useState(0);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };
  return (
    <div className={classes.root}>
      <AppBar position="static">
        <Tabs
          value={value}
          onChange={handleChange}
          aria-label="simple tabs example"
        >
          <Tab label="Feature Data" {...a11yProps(0)} />
          <Tab label="Statistics" {...a11yProps(1)} />
          <Tab label="Feature Sets" {...a11yProps(2)} />
        </Tabs>
      </AppBar>
      <TabPanel value={value} index={0}>
        <Grid container item justify="center" direction="row" lg={12}>
          <div className={classes.detail}>
            <p>
              <b>Version name: </b> {data["version-name"]}
            </p>
            <p>
              <b>Source: </b> {data["source"]}
            </p>
            <p>
              <b>Entity: </b> {data["entity"]}
            </p>
          </div>
          <div className={classes.detail}>
            <p>
              <b>Created: </b> {data["created"]}
            </p>
            <p>
              <b>Last Updated: </b> {data["last-updated"]}
            </p>
          </div>
          <div className={classes.detail}>
            <p>
              <b>Owner: </b> {data["owner"]}
            </p>
            <p>
              <b>Visibility: </b> {data["visibility"]}
            </p>
            <p>
              <b>Revision: </b> {data["revision"]}
            </p>
          </div>
          <div className={classes.detail}>
            <p>
              <b>Description: </b> {data["description"]}
            </p>
          </div>
        </Grid>
      </TabPanel>
      <TabPanel value={value} index={1}>
        <div className={classes.detail}>
          <p>
            <b>Statistical Data: </b> {JSON.stringify(data["statistical data"])}
          </p>
          <FeatureDistribution
            data={data["statistical data"]["distribution-data"]}
          />
        </div>
      </TabPanel>
      <TabPanel value={value} index={2}>
        <div className={classes.detail}>
          <FeatureSetList data={data["feature sets"]} />
        </div>
      </TabPanel>
    </div>
  );
};

export default FeatureDataView;
