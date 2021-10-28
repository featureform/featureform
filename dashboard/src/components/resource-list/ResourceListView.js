import React from "react";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Grid from "@material-ui/core/Grid";
import Chip from "@material-ui/core/Chip";
import MenuItem from "@material-ui/core/MenuItem";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";
import MaterialTable, {
  MTableBody,
  MTableHeader,
  MTableToolbar,
} from "material-table";
import { PrismAsyncLight as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/cjs/languages/prism/python";
import sql from "react-syntax-highlighter/dist/cjs/languages/prism/sql";
import json from "react-syntax-highlighter/dist/cjs/languages/prism/json";
import { useHistory } from "react-router-dom";
import Container from "@material-ui/core/Container";

SyntaxHighlighter.registerLanguage("python", python);
SyntaxHighlighter.registerLanguage("sql", sql);
SyntaxHighlighter.registerLanguage("json", json);

const useStyles = makeStyles((theme) => ({
  root: {
    background: "rgba(255, 255, 255, 0.5)",

    //paddingLeft: "24px",
    //paddingRight: "24px",
    border: "2px solid #F5F6F7",
    borderRadius: 16,
    "& > *": {
      borderRadius: 16,
    },
  },
  table: {
    borderRadius: 16,
    background: "rgba(255, 255, 255, 1)",
    border: "2px solid #F5F6F7",
  },
  detailPanel: {
    padding: theme.spacing(4),
  },
  config: {
    width: "100%",
  },
  detailButton: {
    margin: theme.spacing(1),
  },
  tag: {
    margin: theme.spacing(0.1),
  },
  tableBody: {
    border: "2px solid #F5F6F7",
    background: "white",
    color: "white",
    opacity: 1,
  },
  tableToolbar: {
    paddingTop: theme.spacing(3),
    paddingBottom: theme.spacing(1),
  },
}));

export const ResourceListView = ({
  title,
  resources,
  loading,
  failed,
  activeTags,
  activeVersions = {},
  setVersion,
  toggleTag,
}) => {
  const classes = useStyles();
  let history = useHistory();
  const initialLoad = resources == null && !loading;
  const initRes = resources || [];
  const copy = (res) => res.map((o) => ({ ...o }));
  // MaterialTable can't handle immutable object, we have to make a copy
  // https://github.com/mbrn/material-table/issues/666
  const mutableRes = copy(initRes);

  let myVersions = {};

  mutableRes.forEach((o) => {
    if (!activeVersions[o.name]) {
      myVersions[o.name] = o["default-version"];
    } else {
      myVersions[o.name] = activeVersions[o.name];
    }
  });

  function detailRedirect(e, data) {
    history.push(history.location.pathname + "/" + data.name);
  }

  let versionRes = mutableRes.map((row) => ({
    ...row["versions"][myVersions[row.name]],
    name: row["name"],
  }));

  let rowVersions = mutableRes.map((row) => ({
    name: row["name"],
    versions: row["all-versions"],
  }));

  return (
    <div>
      <MaterialTable
        className={classes.table}
        title={
          <Typography variant="h4">
            <b>{title}</b>
          </Typography>
        }
        columns={[
          { title: "Name", field: "name" },
          { title: "Description", field: "description" },
          {
            title: "Tags",
            field: "tags",
            render: (row) => (
              <TagList
                activeTags={activeTags}
                tags={row.tags}
                tagClass={classes.tag}
                toggleTag={toggleTag}
              />
            ),
          },
          { title: "Revision", field: "revision" },
          {
            title: "Version",
            field: "versions",
            render: (row) => (
              <VersionSelector
                name={row.name}
                versions={
                  rowVersions.find((v) => v.name === row.name)["versions"]
                }
                activeVersions={myVersions}
                setVersion={setVersion}
              />
            ),
          },
        ]}
        data={versionRes}
        isLoading={initialLoad || loading || failed}
        onRowClick={detailRedirect}
        components={{
          Container: (props) => (
            <Container maxWidth="xl" className={classes.root} {...props} />
          ),
          Body: (props) => (
            <MTableBody
              style={{ borderRadius: 16 }}
              className={classes.tableBody}
              {...props}
            />
          ),
          Header: (props) => (
            <MTableHeader className={classes.tableBody} {...props} />
          ),
          Toolbar: (props) => (
            <div className={classes.tableToolbar}>
              <MTableToolbar {...props} />
            </div>
          ),
        }}
        options={{
          search: true,
          draggable: false,
          headerStyle: {
            backgroundColor: "#FFF",
            color: "#F7195C",
            //textColor: "#F7195C",
            marginLeft: 3,
          },
          rowStyle: {
            //backgroundColor: "#FFF",
            opacity: 1,
            borderRadius: 16,
          },
        }}
      />
    </div>
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
        onClick={(event) => {
          toggleTag(tag);
          event.stopPropagation();
        }}
        variant="outlined"
        label={tag}
      />
    ))}
  </Grid>
);

export const VersionSelector = ({
  name,
  versions = [""],
  activeVersions = {},
  setVersion,
  children,
}) => (
  <FormControl>
    <Select
      value={activeVersions[name] || versions[0]}
      onChange={(event) => setVersion(name, event.target.value)}
    >
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

ResourceListView.propTypes = {
  title: PropTypes.string.isRequired,
  resources: PropTypes.array,
  loading: PropTypes.bool,
  failed: PropTypes.bool,
  activeVersions: PropTypes.object,
  setVersion: PropTypes.func,
};

export default ResourceListView;
