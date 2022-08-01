import React from "react";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import { useRouter } from "next/router";
import Typography from "@material-ui/core/Typography";
import Grid from "@material-ui/core/Grid";
import Chip from "@material-ui/core/Chip";
import theme from "../../styles/theme";
import CircleOutlinedIcon from "@mui/icons-material/CircleOutlined";
import Rating from "@mui/material/Rating";
import MaterialTable, {
  MTableBody,
  MTableHeader,
  MTableToolbar,
} from "material-table";
import { PrismAsyncLight as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/cjs/languages/prism/python";
import sql from "react-syntax-highlighter/dist/cjs/languages/prism/sql";
import json from "react-syntax-highlighter/dist/cjs/languages/prism/json";
import Container from "@material-ui/core/Container";
import { providerLogos } from "../../api/resources";
import Button from "@material-ui/core/Button";
import Resource from "../../api/resources/Resource.js";

SyntaxHighlighter.registerLanguage("python", python);
SyntaxHighlighter.registerLanguage("sql", sql);
SyntaxHighlighter.registerLanguage("json", json);

const useStyles = makeStyles(() => ({
  root: {
    background: "rgba(255, 255, 255, 0.5)",
    border: `2px solid ${theme.palette.border.main}`,
    borderRadius: 16,
    "& > *": {
      borderRadius: 16,
    },
  },
  noDataPage: {
    "& > *": {
      padding: theme.spacing(1),
    },
  },
  table: {
    borderRadius: 16,
    background: "rgba(255, 255, 255, 1)",
    border: `2px solid ${theme.palette.border.main}`,
  },
  usageIcon: {
    color: "red",
  },
  variantTableContainer: {
    marginLeft: theme.spacing(4),
    marginRight: theme.spacing(4),
  },
  variantTable: {
    background: "rgba(255, 255, 255, 1)",
    border: `2px solid ${theme.palette.border.main}`,
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
    border: `2px solid ${theme.palette.border.main}`,
    background: "white",
    color: "white",
    opacity: 1,
  },
  providerColumn: {},
  providerLogo: {
    maxWidth: "6em",
    maxHeight: "2.5em",
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
  type,
  activeTags,
  activeVariants = {},
  setVariant,
  toggleTag,
}) => {
  const columnFormats = {
    default: [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
    ],
    Model: [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
    ],
    default_tags: [
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
    ],
    Feature: [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
      {
        title: "Type",
        field: "data-type",
      },
      {
        title: "Default Variant",
        field: "variants",
        render: (row) => (
          <Typography variant="body1">{row["default-variant"]}</Typography>
        ),
      },
    ],
    Provider: [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
      { title: "Type", field: "type" },
      {
        title: "Software",
        field: "software",
        render: (row) => (
          <div className={classes.providerColumn}>
            <img
              alt={row.software}
              className={classes.providerLogo}
              src={providerLogos[row.software.toUpperCase()]}
            ></img>
          </div>
        ),
      },
    ],
    "Data Source": [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
      { title: "Type", field: "type" },
    ],
    User: [{ title: "Name", field: "name" }],
    Entity: [
      { title: "Name", field: "name" },
      {
        title: "Description",
        field: "description",
      },
    ],
    Transformation: [
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
      {
        title: "Default Variant",
        field: "variants",
        render: (row) => (
          <Typography variant="body1">
            {rowVariants.find((v) => v.name === row.name)["default-variant"]}
          </Typography>
        ),
      },
    ],
  };
  const classes = useStyles();
  let router = useRouter();
  const initialLoad = resources == null && !loading;
  const initRes = resources || [];
  const copy = (res) => res.map((o) => ({ ...o }));
  const noVariants = !Resource[type].hasVariants;
  // MaterialTable can't handle immutable object, we have to make a copy
  // https://github.com/mbrn/material-table/issues/666
  const mutableRes = copy(initRes);
  
  function detailRedirect(e, data) {
    router.push(router.query["type"] + "/" + data.name);
  }

  let rowVariants = {};

  return (
    <div>
      <MaterialTable
        {...(!noVariants
          ? {
              detailPanel: (row) => {
                return (
                  <VariantTable
                    name={row.name}
                    row={row}
                    type={type}
                    setVariant={setVariant}
                  />
                );
              },
            }
          : {})}
        className={classes.table}
        title={
          <Typography variant="h4">
            <b>{Resource[type].typePlural}</b>
          </Typography>
        }
        columns={
          Object.keys(columnFormats).includes(title)
            ? columnFormats[title]
            : columnFormats["default"]
        }
        data={mutableRes.map((row) => {
          //mapping each row to have the same object format
          //whether or not resource type has variants
          //Expected format for resource without variants: {"name": <name>, "description": <description> }
          //Expected format for resource with variants:
          ///    {
          //  "default-variant": <default variant>
          // ...data pertaining to active variant (default variant by default)
          //  "variants": <data for all variants> (used in variant dropdown view)
          //}
          let rowData = {};
          if (!row.variants) {
            for (const [key, data] of Object.entries(row)) {
              rowData[key] = data;
            }
            return rowData;
          }
          let rowVariant;
          if (!activeVariants[row.name]) {
            rowVariant = row["default-variant"];
          } else {
            rowVariant = activeVariants[row.name];
          }
          for (const [key, data] of Object.entries(row.variants[rowVariant])) {
            rowData[key] = data;
          }
          let variantList = [];
          Object.values(row.variants).forEach((variantValue) => {
            variantList.push(variantValue);
          });
          rowData["variants"] = variantList;
          rowData["default-variant"] = row["default-variant"];
          return rowData;
        })}
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
            backgroundColor: "white",
            color: theme.palette.primary.main,
            marginLeft: 3,
          },
          rowStyle: {
            opacity: 1,
            borderRadius: 16,
          },
        }}
        {...(!(initialLoad || loading || failed)
          ? {
              localization: {
                body: {
                  emptyDataSourceMessage: <NoDataMessage type={title} />,
                },
              },
            }
          : {})}
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

export const VariantTable = ({ name, setVariant, type, row }) => {
  const classes = useStyles();
  let history = useHistory();
  function variantChangeRedirect(e, data) {
    setVariant(type, name, data.variant);
    history.push(Resource[type].urlPathResource(name));
  }

  let myVariants = [];
  row.variants.forEach((variant) => {
    myVariants.push({
      variant: variant.variant,
      description: variant.description,
    });
  });

  const MAX_ROW_SHOW = 5;
  const ROW_HEIGHT = 5;
  return (
    <div className={classes.variantTableContainer}>
      <MaterialTable
        className={classes.variantTable}
        title={
          <Typography variant="h6">
            <b></b>
          </Typography>
        }
        onRowClick={variantChangeRedirect}
        components={{
          Container: (props) => (
            <Container
              maxWidth="xl"
              className={classes.variantTable}
              {...props}
            />
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
        columns={[
          { title: "Variants", field: "variant" },
          { title: "Description", field: "description" },
        ]}
        data={myVariants}
        options={{
          search: true,
          pageSize: row.variants.length,
          maxHeight: `${MAX_ROW_SHOW * ROW_HEIGHT}em`,
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
            height: `${ROW_HEIGHT}em`,
          },
        }}
      />
    </div>
  );
};

const customIcons = {
  1: {
    icon: <CircleOutlinedIcon />,
    label: "Unused",
  },
  2: {
    icon: <CircleOutlinedIcon />,
    label: "Dissatisfied",
  },
  3: {
    icon: <CircleOutlinedIcon />,
    label: "Neutral",
  },
  4: {
    icon: <CircleOutlinedIcon />,
    label: "Satisfied",
  },
  5: {
    icon: <CircleOutlinedIcon />,
    label: "Frequently used",
  },
};
function IconContainer(props) {
  const { value, ...other } = props;
  return <span {...other}>{customIcons[value].icon}</span>;
}

export const UsageTab = ({ usage, children }) => {
  const classes = useStyles();

  return (
    <Rating
      className={classes.usageIcon}
      name="read-only"
      value={2}
      IconContainerComponent={IconContainer}
      readOnly
    />
  );
};

const NoDataMessage = ({ type }) => {
  const classes = useStyles();

  function redirect() {
    window.location.href = "https://docs.featureform.com/quickstart";
  }
  return (
    <Container>
      <div className={classes.noDataPage}>
        <Typography variant="h4">
          No {Resource[type].typePlural} Registered
        </Typography>
        <Typography variant="body1">
          There are no visible {type.toLowerCase()}s in your organization.
        </Typography>
        <Typography vairant="body1">
          Check out our docs for step by step instructions to create one.
        </Typography>
        <Button variant="outlined" onClick={redirect}>
          FeatureForm Docs
        </Button>
      </div>
    </Container>
  );
};

ResourceListView.propTypes = {
  title: PropTypes.string.isRequired,
  resources: PropTypes.array,
  loading: PropTypes.bool,
  failed: PropTypes.bool,
  activeVariants: PropTypes.object,
  setVariant: PropTypes.func,
};

export default ResourceListView;
