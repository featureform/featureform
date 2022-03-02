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
import Button from "@material-ui/core/Button";
import Chip from "@material-ui/core/Chip";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";
import MenuItem from "@material-ui/core/MenuItem";

import { PrismAsyncLight as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/cjs/languages/prism/python";
import sql from "react-syntax-highlighter/dist/cjs/languages/prism/sql";
import json from "react-syntax-highlighter/dist/cjs/languages/prism/json";
import { okaidia } from "react-syntax-highlighter/dist/cjs/styles/prism";

import VersionControl from "./elements/VersionControl";
import TagBox from "./elements/TagBox";
import MetricsDropdown from "./elements/MetricsDropdown";
import StatsDropdown from "./elements/StatsDropdown";
import { local } from "api/resources";
import Resource from "api/resources/Resource.js";
import theme from "styles/theme/index.js";

SyntaxHighlighter.registerLanguage("python", python);
SyntaxHighlighter.registerLanguage("sql", sql);
SyntaxHighlighter.registerLanguage("json", json);

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
    justifyContent: "space-around",
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
    alignItems: "self-end",
  },
  titleBox: {
    diplay: "inline-block",
    flexDirection: "row",
  },
  entityButton: {
    justifyContent: "left",
    padding: 0,
    width: "30%",
    textTransform: "none",
  },
  transformButton: {
    justifyContent: "left",
    padding: 0,
    //width: "30%",
    textTransform: "none",
  },
  description: {},

  icon: {
    marginRight: theme.spacing(2),
  },
  versionControl: {
    alignSelf: "flex-end",
  },
  syntax: {
    width: "40%",
    paddingLeft: theme.spacing(2),
  },
  resourceList: {
    background: "rgba(255, 255, 255, 0.3)",

    paddingLeft: "0",
    paddingRight: "0",
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
    "& > *": {
      borderRadius: 16,
    },
  },
  typeTitle: {
    paddingRight: theme.spacing(1),
  },
  tableBody: {
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
  },
  linkChip: {
    //width: "10%",
    "& .MuiChip-label": {
      paddingRight: theme.spacing(0),
    },
  },
  linkBox: {
    display: "flex",
  },
  tableHeader: {
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
    color: theme.palette.border.alternate,
  },
  tabChart: {
    "& .MuiBox-root": {
      padding: "0",
      margin: "0",
      paddingTop: "1em",
      paddingBottom: "0.5em",
    },
  },
  config: {
    flexGrow: 1,
    paddingLeft: theme.spacing(2),
    marginTop: theme.spacing(2),
    borderLeft: `3px solid ${theme.palette.secondary.main}`,
    marginLeft: theme.spacing(2),
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
    paddingLeft: "1em",
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
  let resourceType = Resource[entity.resources.type];
  let type = resourceType.type;
  const showMetrics = resourceType.hasMetrics;
  const singleVariant = !resourceType.hasVariants;
  const showStats = false;
  const dataTabDisplacement = (1 ? showMetrics : 0) + (1 ? showStats : 0);
  const statsTabDisplacement = showMetrics ? 1 : 0;
  const name = resources["name"];
  const icon = resourceType.materialIcon;
  const enableTags = false;

  let version = resources["default-variant"];

  if (activeVersions[entity.resources.type][name]) {
    version = activeVersions[entity.resources.type][name];
  } else {
    setVersion(entity.resources.type, name, resources["default-variant"]);
  }

  let resource;
  if (resourceType.hasVariants) {
    resource = resources.versions[version];
  } else {
    resource = resources;
  }
  let metadata = {};
  let resourceData = {};

  Object.keys(resource).forEach((key) => {
    if (Resource.pathToType[key]) {
      resourceData[Resource.pathToType[key]] = resource[key];
    } else {
      metadata[key] = resource[key];
    }
  });
  if (metadata["source"]) {
    metadata["source"] = metadata["source"].Name;
    metadata["source-variant"] = metadata["source"].Variant;
  }

  const convertTimestampToDate = (timestamp_string) => {
    return new Date(timestamp_string).toLocaleString("en-US", {
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    });
  };

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

  const linkToEntityPage = (event) => {
    history.push(`/entities/${metadata["entity"]}`);
  };

  const linkToTransformSource = (event) => {
    history.push(`/transformations/${metadata["transformation source"]}`);
  };

  const linkToDataSource = (event) => {
    history.push(`/primary-data/${metadata["primary data"]}`);
  };

  const linkToUserPage = (event) => {
    history.push(`/users/${metadata["owner"]}`);
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
                    {metadata["created"] && (
                      <Typography variant="subtitle1">
                        Created: {convertTimestampToDate(metadata["created"])}
                      </Typography>
                    )}
                  </div>
                </div>
                {allVersions && allVersions.length > 1 && (
                  <VersionControl
                    version={version}
                    versions={allVersions}
                    handleVersionChange={handleVersionChange}
                    type={type}
                    name={name}
                    local={local}
                    convertTimestampToDate={convertTimestampToDate}
                  />
                )}
              </div>
            </Grid>
          </Grid>
          {Object.keys(metadata).length > 0 && (
            <div className={classes.resourceData}>
              <Grid container spacing={0}>
                <Grid item xs={7} className={classes.resourceMetadata}>
                  {metadata["description"] && (
                    <Typography variant="body1" className={classes.description}>
                      <b>Description:</b> {metadata["description"]}
                    </Typography>
                  )}

                  {metadata["owner"] && (
                    <div className={classes.linkBox}>
                      <Typography variant="body1" className={classes.typeTitle}>
                        <b>Owner:</b>{" "}
                      </Typography>
                      <Chip
                        variant="outlined"
                        className={classes.linkChip}
                        size="small"
                        onClick={linkToUserPage}
                        className={classes.transformButton}
                        label={metadata["owner"]}
                      ></Chip>
                    </div>
                  )}

                  {metadata["dimensions"] && (
                    <Typography variant="body1">
                      <b>Dimensions:</b> {metadata["dimensions"]}
                    </Typography>
                  )}
                  {metadata["type"] && (
                    <Typography variant="body1">
                      <b>Type:</b> {metadata["type"]}
                    </Typography>
                  )}
                  {metadata["joined"] && (
                    <Typography variant="body1">
                      <b>Joined:</b>{" "}
                      {convertTimestampToDate(metadata["joined"])}
                    </Typography>
                  )}
                  {metadata["software"] && (
                    <Typography variant="body1">
                      <b>Software:</b> {metadata["software"]}
                    </Typography>
                  )}
                  {metadata["team"] && (
                    <Typography variant="body1">
                      <b>Team:</b> {metadata["team"]}
                    </Typography>
                  )}
                  {metadata["source"] && (
                    <Typography variant="body1">
                      <b>Source:</b> {metadata["source"]}
                    </Typography>
                  )}

                  {metadata["primary data"] && (
                    <div className={classes.linkBox}>
                      <Typography variant="body1" className={classes.typeTitle}>
                        <b>Primary Data: </b>{" "}
                      </Typography>
                      <Chip
                        variant="outlined"
                        className={classes.linkChip}
                        size="small"
                        onClick={linkToDataSource}
                        className={classes.transformButton}
                        label={metadata["primary data"]}
                      ></Chip>
                    </div>
                  )}

                  {metadata["entity"] && (
                    <div className={classes.linkBox}>
                      <Typography variant="body1" className={classes.typeTitle}>
                        <b>Entity:</b>{" "}
                      </Typography>
                      <Chip
                        variant="outlined"
                        className={classes.linkChip}
                        size="small"
                        onClick={linkToEntityPage}
                        className={classes.transformButton}
                        label={metadata["entity"]}
                      ></Chip>
                    </div>
                  )}
                </Grid>
                <Grid item xs={2}></Grid>
                {enableTags && (
                  <Grid item xs={3}>
                    {metadata["tags"] && <TagBox tags={metadata["tags"]} />}
                  </Grid>
                )}
              </Grid>
            </div>
          )}
          {metadata["config"] && (
            <div className={classes.config}>
              <Typography variant="body1">
                <b>Config:</b>
              </Typography>
              <SyntaxHighlighter
                className={classes.syntax}
                language={metadata["language"]}
                style={okaidia}
              >
                {metadata["config"]}
              </SyntaxHighlighter>
            </div>
          )}
        </div>

        <div className={classes.root}>
          <AppBar position="static" className={classes.appbar}>
            <Tabs
              value={value}
              onChange={handleChange}
              aria-label="simple tabs example"
            >
              {showMetrics && <Tab label={"metrics"} {...a11yProps(0)} />}
              {showStats && (
                <Tab label={"stats"} {...a11yProps(statsTabDisplacement)} />
              )}
              {Object.keys(resourceData).map((key, i) => (
                <Tab
                  label={Resource[key].typePlural}
                  {...a11yProps(i + dataTabDisplacement)}
                />
              ))}
            </Tabs>
          </AppBar>
          {showMetrics && (
            <TabPanel
              className={classes.tabChart}
              value={value}
              key={"metrics"}
              index={0}
              classes={{
                root: classes.tabChart,
              }}
            >
              <MetricsDropdown type={type} name={name} version={version} />
            </TabPanel>
          )}
          {showStats && (
            <TabPanel
              className={classes.tabChart}
              value={value}
              key={"stats"}
              index={statsTabDisplacement}
              classes={{
                root: classes.tabChart,
              }}
            >
              <StatsDropdown type={type} name={name} />
            </TabPanel>
          )}

          {Object.keys(resourceData).map((key, i) => (
            <TabPanel
              className={classes.tabChart}
              value={value}
              key={key}
              index={i + dataTabDisplacement}
              classes={{
                root: classes.tabChart,
              }}
            >
              <MaterialTable
                className={classes.tableRoot}
                {...(!singleVariant
                  ? {
                      detailPanel: (row) => {
                        return (
                          <VersionTable
                            type={key}
                            name={row.name}
                            versions={row.variants}
                            setVersion={setVersion}
                          />
                        );
                      },
                    }
                  : {})}
                title={capitalize(key)}
                options={{
                  toolbar: false,
                  headerStyle: {
                    backgroundColor: theme.palette.border.main,
                    marginLeft: 3,
                  },
                }}
                {...(Object.keys(resourceData[key]).length > 0
                  ? {
                      columns: Object.keys(resourceData[key][0])
                        .filter((item) => item != "tags" && item != "variants")
                        .map((item) => ({
                          title: capitalize(item),
                          field: item,
                          ...(item == "variants" && {
                            render: (row) => (
                              <VersionSelector
                                name={row.name}
                                versions={row.variants}
                              />
                            ),
                          }),
                          ...(item == "tags" && {
                            render: (row) => (
                              <TagList tags={row.tags} tagClass={classes.tag} />
                            ),
                          }),
                        })),
                    }
                  : {})}
                data={resourceData[key].map((o) => {
                  let new_object = {};
                  Object.keys(o).forEach((key) => {
                    if (convertTimestampToDate(o[key]) != "Invalid Date") {
                      new_object[key] = convertTimestampToDate(o[key]);
                    } else {
                      new_object[key] = o[key];
                    }
                  });
                  return new_object;
                })}
                onRowClick={(event, rowData) =>
                  history.push(Resource[key].urlPathResource(rowData.Name))
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

export const TagList = ({
  activeTags = {},
  tags = [],
  tagClass,
  toggleTag,
}) => (
  <Grid container direction="row">
    {tags.map((tag) => (
      <Chip
        key={tag}
        className={tagClass}
        color={activeTags[tag] ? "secondary" : "default"}
        onClick={(event) => {}}
        variant="outlined"
        label={tag}
      />
    ))}
  </Grid>
);

export const VersionSelector = ({ name, versions = [""], children }) => (
  <FormControl>
    <Select value={versions[0]}>
      {versions.map((version) => (
        <MenuItem
          key={version}
          value={version}
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          {version}
        </MenuItem>
      ))}
    </Select>
  </FormControl>
);

export const VersionTable = ({
  name,
  versions = [""],
  type,
  activeVersions,
  setVersion,
  children,
  mutableRes,
}) => {
  const classes = useStyles();
  let history = useHistory();
  function versionChangeRedirect(e, data) {
    setVersion(type, name, data.variant);
    history.push(Resource[type].urlPathResource(name));
  }
  let myVariants = [];
  versions.forEach((version) => {
    myVariants.push({ variant: version });
  });
  return (
    <div>
      <MaterialTable
        className={classes.table}
        title={
          <Typography variant="h6">
            <b></b>
          </Typography>
        }
        onRowClick={versionChangeRedirect}
        columns={[
          { title: "Variants", field: "variant" },
          { title: "Description", field: "description" },
        ]}
        data={myVariants}
        options={{
          search: true,
          toolbar: false,
          draggable: false,
          headerStyle: {
            backgroundColor: "white",
            color: theme.palette.primary.main,
            marginLeft: 3,
          },
          rowStyle: {
            opacity: 1,
            borderRadius: 16,
          },
        }}
      />
    </div>
  );
};

export default EntityPageView;
