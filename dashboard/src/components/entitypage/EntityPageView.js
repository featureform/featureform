import React from "react";
import AppBar from "@material-ui/core/AppBar";
import Tabs from "@material-ui/core/Tabs";
import Tab from "@material-ui/core/Tab";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import MaterialTable from "material-table";
import Grid from "@material-ui/core/Grid";
import { useHistory } from "react-router-dom";
import Container from "@material-ui/core/Container";
import Paper from "@material-ui/core/Paper";
import Avatar from "@material-ui/core/Avatar";
import Icon from "@material-ui/core/Icon";

import VersionControl from "./elements/VersionControl";
import TagBox from "./elements/TagBox";
import MetricsDropdown from "./elements/MetricsDropdown";
import { resourceIcons } from "api/resources";

function TabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && <Box p={3}>{children}</Box>}
    </div>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.any.isRequired,
  value: PropTypes.any.isRequired,
};
const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
    padding: theme.spacing(0),
    backgroundColor: theme.palette.background.paper,
  },
  metadata: {
    padding: theme.spacing(2),
  },
  small: {
    width: theme.spacing(3),
    height: theme.spacing(3),
    display: "inline-flex",
  },
  titleBox: {
    diplay: "inline-block",
    flexDirection: "row",
    marginBottom: 7,
  },
  description: {
    marginBottom: 15,
  },
  owner: {
    marginBottom: 7,
  },
  icon: {
    marginRight: theme.spacing(2),
  },
  versionControl: {
    alignSelf: "flex-end",
  },

  resourceData: {
    flexGrow: 2,
    padding: theme.spacing(2),
  },
}));

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    "aria-controls": `simple-tabpanel-${index}`,
  };
}

const EntityPageView = ({ entity, setVersion, activeVersions }) => {
  let history = useHistory();
  let resources = entity.resources;

  const type = resources["type"];
  const name = resources["name"];
  const icon = resourceIcons[type];

  let version = resources["default-version"];

  if (activeVersions[type][name]) {
    version = activeVersions[type][name];
  } else {
    setVersion(type, name, resources["default-version"]);
  }

  let resource = resources.versions[version];
  const metadata = resource.metadata;
  const resourceData = resource.data;

  let allVersions = resources["all-versions"];

  const classes = useStyles();
  const [value, setValue] = React.useState(0);

  const handleVersionChange = (event) => {
    setVersion(type, name, event.target.value);
  };

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  const capitalize = (word) => {
    return word[0].toUpperCase() + word.slice(1).toLowerCase();
  };

  return true || (!resources.loading && !resources.failed && resources.data) ? (
    <div>
      <Container maxWidth="xl">
        <Paper elevation={3}>
          <div className={classes.metadata}>
            <Grid
              container
              className={classes.topContainer}
              lg={12}
              justifyContent="flex-start"
            >
              <Grid item xs={false} className={classes.icon}>
                <Icon>{icon}</Icon>
              </Grid>
              <Grid item xs={9} lg={8}>
                <Typography variant="h4" component="h4">
                  {resources.name}
                </Typography>
                <Typography variant="subtitle1">
                  Last updated: {metadata["revision"]}
                </Typography>
              </Grid>

              <Grid item xs={2} className={classes.versionControl}>
                <VersionControl
                  version={version}
                  versions={allVersions}
                  handleVersionChange={handleVersionChange}
                  type={type}
                  name={name}
                />
              </Grid>
            </Grid>
            <Paper>
              <div className={classes.resourceData}>
                <Grid container spacing={0}>
                  <Grid item xs={7}>
                    <Typography variant="body1" className={classes.description}>
                      {metadata["description"]}
                    </Typography>

                    <div className={classes.titleBox}>
                      <Typography display="inline" variant="body1">
                        Owner:{"  "}
                      </Typography>
                      <Avatar
                        alt={metadata["owner"]}
                        src="/static/images/avatar/1.jpg"
                        className={classes.small}
                      />
                    </div>
                    <div className={classes.owner}></div>
                    {metadata["dimensions"] ? (
                      <Typography variant="body1">
                        Dimensions: {metadata["dimensions"]}
                      </Typography>
                    ) : (
                      <Typography></Typography>
                    )}
                    {metadata["source"] ? (
                      <Typography variant="body1">
                        Source: {metadata["source"]}
                      </Typography>
                    ) : (
                      <Typography></Typography>
                    )}

                    {metadata["entity"] ? (
                      <Typography variant="body1">
                        Entity: {metadata["entity"]}
                      </Typography>
                    ) : (
                      <Typography></Typography>
                    )}
                  </Grid>
                  <Grid item xs={2}></Grid>
                  <Grid item xs={3}>
                    <TagBox tags={metadata["tags"]} />
                  </Grid>
                </Grid>
              </div>
            </Paper>
          </div>
          <div className={classes.root}>
            <Paper>
              <AppBar position="static">
                <Tabs
                  value={value}
                  onChange={handleChange}
                  aria-label="simple tabs example"
                >
                  <Tab label={"metrics"} {...a11yProps(0)} />
                  {Object.keys(resourceData).map((key, i) => (
                    <Tab label={key} {...a11yProps(i + 1)} />
                  ))}
                </Tabs>
              </AppBar>
              <TabPanel
                className={classes.tabChart}
                value={value}
                key={"metrics"}
                index={0}
              >
                <MetricsDropdown />
              </TabPanel>

              {Object.keys(resourceData).map((key, i) => (
                <TabPanel value={value} key={key} index={i + 1}>
                  <MaterialTable
                    title={capitalize(key)}
                    options={{
                      toolbar: false,
                    }}
                    columns={Object.keys(resourceData[key][0]).map((item) => ({
                      title: capitalize(item),
                      field: item,
                    }))}
                    data={resourceData[key].map((o) => ({ ...o }))}
                    onRowClick={(event, rowData) =>
                      history.push("/" + key + "/" + rowData.name)
                    }
                  />
                </TabPanel>
              ))}
            </Paper>
          </div>
        </Paper>
      </Container>
    </div>
  ) : (
    <div></div>
  );
};

export default EntityPageView;
