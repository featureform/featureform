import React from "react";
import AppBar from "@material-ui/core/AppBar";
import Tabs from "@material-ui/core/Tabs";
import Tab from "@material-ui/core/Tab";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import MaterialTable, { MTableBody, MTableHeader } from "material-table";
import Box from "@material-ui/core/Box";
import Grid from "@material-ui/core/Grid";
import { useHistory } from "react-router-dom";
import Container from "@material-ui/core/Container";
import Avatar from "@material-ui/core/Avatar";
import Icon from "@material-ui/core/Icon";

import VersionControl from "./elements/VersionControl";
import TagBox from "./elements/TagBox";
import MetricsDropdown from "./elements/MetricsDropdown";
import { resourceIcons } from "api/resources";
import theme from "styles/theme/index.js";

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
    marginTop: theme.spacing(2),
  },
  resourceMetadata: {
    padding: theme.spacing(1),
    display: "flex",
    flexDirection: "column",
    justifyContent: "space-between",
  },
  border: {
    background: "white",
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: "16px",
  },
  data: {
    background: "white",
    marginTop: theme.spacing(2),
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: "16px",
  },
  appbar: {
    background: "transparent",
    boxShadow: "none",
    color: "black",
  },
  metadata: {
    marginTop: theme.spacing(2),
    padding: theme.spacing(1),
  },
  small: {
    width: theme.spacing(3),
    height: theme.spacing(3),
    display: "inline-flex",
  },
  titleBox: {
    diplay: "inline-block",
    flexDirection: "row",
  },
  description: {},

  icon: {
    marginRight: theme.spacing(2),
  },
  versionControl: {
    alignSelf: "flex-end",
  },
  resourceList: {
    background: "rgba(255, 255, 255, 0.3)",

    paddingLeft: "0px",
    paddingRight: "0px",
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
    "& > *": {
      borderRadius: 16,
    },
  },
  tableBody: {
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
  },
  tableHeader: {
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
    color: theme.palette.border.alternate,
  },
  tabChart: {
    "& .MuiBox-root": {
      padding: "0px",
      margin: "0px",
      paddingTop: "24px",
      paddingBottom: "24px",
    },
  },

  resourceData: {
    flexGrow: 1,
    paddingLeft: theme.spacing(1),
    borderLeft: `3px solid ${theme.palette.secondary.main}`,
    marginLeft: theme.spacing(2),
  },
  tableRoot: {
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
  },
  resourcesTopRow: {
    display: "flex",
    justifyContent: "space-between",
  },
  title: {
    display: "flex",
  },
  titleText: {
    paddingLeft: "12px",
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
      <Container maxWidth="xl" className={classes.border}>
        <div className={classes.metadata}>
          <Grid
            container
            className={classes.topContainer}
            lg={12}
            justifyContent="flex-start"
          >
            <Grid item xs={false} className={classes.icon}></Grid>
            <Grid item xs={12} lg={12}>
              <div className={classes.resourcesTopRow}>
                <div className={classes.title}>
                  <Icon>{icon}</Icon>
                  <div className={classes.titleText}>
                    <Typography variant="h4" component="h4">
                      <b>{resources.name}</b>
                    </Typography>
                    <Typography variant="subtitle1">
                      Last updated: {metadata["revision"]}
                    </Typography>
                  </div>
                </div>
                <VersionControl
                  version={version}
                  versions={allVersions}
                  handleVersionChange={handleVersionChange}
                  type={type}
                  name={name}
                />
              </div>
            </Grid>
          </Grid>
          <div className={classes.resourceData}>
            <Grid container spacing={0}>
              <Grid item xs={7} className={classes.resourceMetadata}>
                <Typography variant="body1" className={classes.description}>
                  <b>Description:</b> {metadata["description"]}
                </Typography>

                <div className={classes.titleBox}>
                  <Typography display="inline" variant="body1">
                    <b>Owner:</b>
                    {"  "}
                  </Typography>
                  <Avatar
                    alt={metadata["owner"]}
                    src="/static/images/avatar/1.jpg"
                    className={classes.small}
                  />
                </div>
                {metadata["dimensions"] && (
                  <Typography variant="body1">
                    <b>Dimensions:</b> {metadata["dimensions"]}
                  </Typography>
                )}
                {metadata["source"] && (
                  <Typography variant="body1">
                    <b>Source:</b> {metadata["source"]}
                  </Typography>
                )}

                {metadata["entity"] && (
                  <Typography variant="body1">
                    <b>Entity:</b> {metadata["entity"]}
                  </Typography>
                )}
              </Grid>
              <Grid item xs={2}></Grid>
              <Grid item xs={3}>
                <TagBox tags={metadata["tags"]} />
              </Grid>
            </Grid>
          </div>
        </div>
        <div className={classes.root}>
          <AppBar position="static" className={classes.appbar}>
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
            classes={{
              root: classes.tabChart,
            }}
          >
            <MetricsDropdown />
          </TabPanel>

          {Object.keys(resourceData).map((key, i) => (
            <TabPanel
              className={classes.tabChart}
              value={value}
              key={key}
              index={i + 1}
              classes={{
                root: classes.tabChart,
              }}
            >
              <MaterialTable
                className={classes.tableRoot}
                title={capitalize(key)}
                options={{
                  toolbar: false,
                  headerStyle: {
                    backgroundColor: theme.palette.border.main,
                    marginLeft: 3,
                  },
                }}
                columns={Object.keys(resourceData[key][0]).map((item) => ({
                  title: capitalize(item),
                  field: item,
                }))}
                data={resourceData[key].map((o) => ({ ...o }))}
                onRowClick={(event, rowData) =>
                  history.push("/" + key + "/" + rowData.name)
                }
                components={{
                  Container: (props) => (
                    <div
                      className={classes.resourceList}
                      minWidth="xl"
                      {...props}
                    />
                  ),
                  Body: (props) => (
                    <MTableBody className={classes.tableBody} {...props} />
                  ),
                  Header: (props) => (
                    <MTableHeader className={classes.tableHeader} {...props} />
                  ),
                }}
              />
            </TabPanel>
          ))}
        </div>
      </Container>
    </div>
  ) : (
    <div></div>
  );
};

export default EntityPageView;
