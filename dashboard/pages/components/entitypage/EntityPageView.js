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
// import { useHistory } from "react-router-dom";
import Container from "@material-ui/core/Container";
import Icon from "@material-ui/core/Icon";
import Chip from "@material-ui/core/Chip";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";
import MenuItem from "@material-ui/core/MenuItem";
import { VariantTable } from "../resource-list/ResourceListView.js";

import { PrismAsyncLight as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/cjs/languages/prism/python";
import sql from "react-syntax-highlighter/dist/cjs/languages/prism/sql";
import json from "react-syntax-highlighter/dist/cjs/languages/prism/json";
import { okaidia } from "react-syntax-highlighter/dist/cjs/styles/prism";

import VariantControl from "./elements/VariantControl";
import TagBox from "./elements/TagBox";
import MetricsDropdown from "./elements/MetricsDropdown";
import StatsDropdown from "./elements/StatsDropdown";
import Resource from "../../api/resources/Resource.js";
import theme from "../../styles/theme/index.js";

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
  variantControl: {
    alignSelf: "flex-end",
  },
  syntax: {
    width: "200px + 50%",
    minWidth: "200px",
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
      paddingRight: theme.spacing(1),
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

  resourcesData: {
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

const EntityPageView = ({ entity, setVariant, activeVariants }) => {
  let history = useHistory();
  let resources = entity.resources;
  let resourceType = Resource[entity.resources.type];
  let type = resourceType.type;
  const showMetrics = resourceType.hasMetrics;
  const showStats = false;
  const dataTabDisplacement = (1 ? showMetrics : 0) + (1 ? showStats : 0);
  const statsTabDisplacement = showMetrics ? 1 : 0;
  const name = resources["name"];
  const icon = resourceType.materialIcon;
  const enableTags = false;

  let variant = resources["default-variant"];

  if (activeVariants[entity.resources.type][name]) {
    variant = activeVariants[entity.resources.type][name];
  } else {
    setVariant(entity.resources.type, name, resources["default-variant"]);
  }

  let resource;
  if (resourceType.hasVariants) {
    resource = resources.variants[variant];
  } else {
    resource = resources;
  }
  let metadata = {};
  let resourcesData = {};

  Object.keys(resource).forEach((key) => {
    if (Resource.pathToType[key]) {
      resourcesData[Resource.pathToType[key]] = resource[key];
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

  let allVariants = resources["all-variants"];

  const classes = useStyles();
  const [value, setValue] = React.useState(0);

  const handleVariantChange = (event) => {
    setVariant(type, name, event.target.value);
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

  const linkToPrimaryData = (event) => {
    history.push(`/sources/${metadata["source"]}`);
  };

  const linkToLabel = (event) => {
    history.push(`/labels/${metadata["label"].Name}`);
  };

  const linkToUserPage = (event) => {
    history.push(`/users/${metadata["owner"]}`);
  };

  const linkToProviderPage = (event) => {
    history.push(`/providers/${metadata["provider"]}`);
  };

  return true || (!resources.loading && !resources.failed && resources.data) ? (
    <div>
      <Container maxWidth="xl" className={classes.border}>
        <div className={classes.metadata}>
          <Grid
            container
            className={classes.topContainer}
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
                {allVariants && (
                  <VariantControl
                    variant={variant}
                    variants={allVariants}
                    handleVariantChange={handleVariantChange}
                    type={type}
                    name={name}
                    convertTimestampToDate={convertTimestampToDate}
                  />
                )}
              </div>
            </Grid>
          </Grid>
          {(Object.keys(metadata).length > 0 && metadata["status"] != "NO_STATUS")&& (
            <div className={classes.resourcesData}>
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
                        label={metadata["owner"]}
                      ></Chip>
                    </div>
                  )}

                  {metadata["provider"] && (
                    <div className={classes.linkBox}>
                      <Typography variant="body1" className={classes.typeTitle}>
                        <b>Provider:</b>{" "}
                      </Typography>
                      <Chip
                        variant="outlined"
                        className={classes.linkChip}
                        size="small"
                        onClick={linkToProviderPage}
                        label={metadata["provider"]}
                      ></Chip>
                    </div>
                  )}

                  {metadata["dimensions"] && (
                    <Typography variant="body1">
                      <b>Dimensions:</b> {metadata["dimensions"]}
                    </Typography>
                  )}
                  {metadata["data-type"] && (
                    <Typography variant="body1">
                      <b>Data Type:</b> {metadata["data-type"]}
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
                  {metadata["label"] && (
                    <div className={classes.linkBox}>
                      <Typography variant="body1" className={classes.typeTitle}>
                        <b>Label: </b>{" "}
                      </Typography>
                      <Chip
                        variant="outlined"
                        className={classes.linkChip}
                        size="small"
                        onClick={linkToLabel}
                        label={metadata["label"].Name}
                      ></Chip>
                    </div>
                  )}
                  {metadata["provider-type"] && (
                    <Typography variant="body1">
                      <b>Provider Type:</b> {metadata["provider-type"]}
                    </Typography>
                  )}
                  {metadata["team"] && (
                    <Typography variant="body1">
                      <b>Team:</b> {metadata["team"]}
                    </Typography>
                  )}
                  {metadata["status"] && metadata["status"] !== "NO_STATUS" && (
                    <Typography variant="body1">
                      <b>Status:</b> {metadata["status"]}
                    </Typography>
                  )}
                  {metadata["error"] && metadata["error"] !== "" && (
                    <Typography variant="body1">
                      <b>Error Message:</b> {metadata["error"]}
                    </Typography>
                  )}
                  {metadata["source-type"] && (
                    <Typography variant="body1">
                      <b>Source Type:</b> {metadata["source-type"]}
                    </Typography>
                  )}
                  {metadata["definition"] && (
                    <div>
                      <Typography variant="body1">
                        <b>Origin:</b>
                      </Typography>
                      {metadata["source-type"] === "Transformation" ? (
                        <SyntaxHighlighter
                          className={classes.syntax}
                          language={"sql"}
                          style={okaidia}
                        >
                          {metadata["definition"]}
                        </SyntaxHighlighter>
                      ) : (
                        <Typography variant="h7">
                          <b>{metadata["definition"]}</b>
                        </Typography>
                      )}
                    </div>
                  )}
                  {metadata["serialized-config"] && (
                    <Typography variant="body1">
                      <b>Serialized Config:</b> {metadata["serialized-config"]}
                    </Typography>
                  )}

                  {metadata["source"] && (
                    <div className={classes.linkBox}>
                      <Typography variant="body1" className={classes.typeTitle}>
                        <b>Source: </b>{" "}
                      </Typography>
                      <Chip
                        variant="outlined"
                        className={classes.linkChip}
                        size="small"
                        onClick={linkToPrimaryData}
                        label={metadata["source"]}
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
                        label={metadata["entity"]}
                      ></Chip>
                    </div>
                  )}

                  {metadata["location"] && (
                    <div className={classes.linkBox}>
                      <Typography variant="body1" className={classes.typeTitle}>
                        <b>Columns:</b>{" "}
                      </Typography>
                      <Typography variant="body2">
                        &nbsp;<b>Entity:</b> {metadata["location"].Entity}
                        &nbsp;<b>Value:</b> {metadata["location"].Value}
                        &nbsp;<b>Timestamp:</b> {metadata["location"].TS}
                      </Typography>
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
              {Object.keys(resourcesData).map((key, i) => (
                <Tab
                  key={i}
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
              <MetricsDropdown type={type} name={name} variant={variant} />
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

          {Object.keys(resourcesData).map((resourceType, i) => (
            <TabPanel
              className={classes.tabChart}
              value={value}
              key={resourceType}
              index={i + dataTabDisplacement}
              classes={{
                root: classes.tabChart,
              }}
            >
              <MaterialTable
                className={classes.tableRoot}
                detailPanel={(row) => {
                  return (
                    <VariantTable
                      type={resourceType}
                      name={row.name}
                      row={row}
                      setVariant={setVariant}
                    />
                  );
                }}
                title={capitalize(resourceType)}
                options={{
                  toolbar: false,
                  headerStyle: {
                    backgroundColor: theme.palette.border.main,
                    marginLeft: 3,
                  },
                }}
                {...(Object.keys(resourcesData[resourceType]).length > 0
                  ? {
                      columns: ["name", "variant"].map((item) => ({
                        title: capitalize(item),
                        field: item,
                      })),
                    }
                  : {})}
                data={Object.entries(resourcesData[resourceType]).map(
                  (resourceEntry) => {
                    const resourceName = resourceEntry[0];
                    const resourceVariants = resourceEntry[1];
                    let rowData = { name: resourceName };
                    if (resourceVariants.length === 1) {
                      rowData["variant"] = resourceVariants[0].variant;
                    } else {
                      rowData["variant"] = "...";
                    }
                    rowData["variants"] = Object.values(resourceVariants);
                    return rowData;
                  }
                )}
                onRowClick={(event, rowData) =>
                  history.push(
                    Resource[resourceType].urlPathResource(rowData.name)
                  )
                }
                components={{
                  Container: (props) => (
                    <div
                      className={classes.resourceList}
                      minwidth="xl"
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

export const VariantSelector = ({ name, variants = [""], children }) => (
  <FormControl>
    <Select value={variants[0]}>
      {variants.map((variant) => (
        <MenuItem
          key={variant}
          value={variant}
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          {variant}
        </MenuItem>
      ))}
    </Select>
  </FormControl>
);

export default EntityPageView;
