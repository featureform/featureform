import React from "react";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Grid from "@material-ui/core/Grid";
import Chip from "@material-ui/core/Chip";
import MenuItem from "@material-ui/core/MenuItem";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";
import theme from "styles/theme";
import { useParams } from "react-router-dom";
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
import { useHistory } from "react-router-dom";
import Container from "@material-ui/core/Container";
import { providerLogos } from "api/resources";
import Button from "@material-ui/core/Button";
import Resource from "api/resources/Resource.js";

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
  },
  tableToolbar: {
    paddingTop: theme.spacing(3),
    paddingBottom: theme.spacing(1),
  },
}));

const convertTimestampToDate = (timestamp_string) => {
  return new Date(timestamp_string).toDateString();
};

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
  console.log(type);
  const columnFormats = {
    default: [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
      {
        title: "Usage",
        field: "usage",
        render: (row) => <UsageTab />,
      },
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
      {
        title: "Default Variant",
        field: "variants",
        render: (row) => (
          <VariantSelector
            name={row.name}
            variants={rowVariants.find((v) => v.name === row.name)["variants"]}
            activeVariants={myVariants}
            setVariant={setVariant}
          />
        ),
      },
    ],
    Feature: [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
      {
        title: "Usage",
        field: "usage",
        render: (row) => <UsageTab />,
      },
      {
        title: "Type",
        field: "type",
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
    Provider: [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
      { title: "Type", field: "type" },
      {
        title: "Usage",
        field: "usage",
        render: (row) => <UsageTab />,
      },
      {
        title: "Software",
        field: "software",
        render: (row) => (
          <div className={classes.providerColumn}>
            <img
              className={classes.providerLogo}
              src={providerLogos[row.software]}
            ></img>
          </div>
        ),
      },
    ],
    "Data Source": [
      { title: "Name", field: "name" },
      { title: "Description", field: "description" },
      {
        title: "Usage",
        field: "usage",
        render: (row) => <UsageTab />,
      },
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
  let history = useHistory();
  const initialLoad = resources == null && !loading;
  const initRes = resources || [];
  const copy = (res) => res.map((o) => ({ ...o }));
  const noVariants = !Resource[type].hasVariants;
  // MaterialTable can't handle immutable object, we have to make a copy
  // https://github.com/mbrn/material-table/issues/666
  const mutableRes = copy(initRes);

  let myVariants = {};

  mutableRes.forEach((o) => {
    if (!activeVariants[o.name]) {
      myVariants[o.name] = o["default-variant"];
    } else {
      myVariants[o.name] = activeVariants[o.name];
    }
  });

  function detailRedirect(e, data) {
    history.push(history.location.pathname + "/" + data.name);
  }

  let variantRes = {};
  let rowVariants = {};
  if (noVariants) {
    variantRes = mutableRes;
  } else {
    variantRes = mutableRes.map((row) => ({
      ...row["variants"][myVariants[row.name]],
      name: row["name"],
      revision: convertTimestampToDate(
        row["variants"][myVariants[row.name]]
          ? row["variants"][myVariants[row.name]]["revision"]
          : ""
      ),
    }));
    rowVariants = mutableRes.map((row) => ({
      name: row["name"],
      "default-variant": row["default-variant"],
      variants: row["all-variants"],
    }));
  }

  let default_columns = [
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
      title: "Variant",
      field: "variants",
      render: (row) => (
        <VariantSelector
          name={row.name}
          variants={rowVariants.find((v) => v.name === row.name)["variants"]}
          activeVariants={myVariants}
          setVariant={setVariant}
        />
      ),
    },
  ];

  let provider_columns = [
    { title: "Name", field: "name" },
    { title: "Description", field: "description" },
    { title: "Type", field: "type" },
    {
      title: "Software",
      field: "software",
      render: (row) => (
        <div className={classes.providerColumn}>
          <img
            className={classes.providerLogo}
            src={providerLogos[row.software]}
          ></img>
        </div>
      ),
    },
    { title: "Team", field: "team" },
  ];

  return (
    <div>
      <MaterialTable
        {...(!noVariants
          ? {
              detailPanel: (row) => {
                return (
                  <VariantTable
                    name={row.name}
                    variants={
                      rowVariants.find((v) => v.name === row.name)["variants"]
                    }
                    activeVariants={myVariants}
                    setVariant={setVariant}
                    mutableRes={mutableRes}
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
        data={variantRes}
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

export const VariantSelector = ({
  name,
  variants = [""],
  activeVariants = {},
  setVariant,
  children,
}) => {
  return (
    <FormControl>
      <Select
        value={activeVariants[name] || variants[0]}
        onChange={(event) => setVariant(name, event.target.value)}
      >
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
};

export const VariantTable = ({
  name,
  variants = [""],
  activeVariants,
  setVariant,
  children,
  mutableRes,
}) => {
  const classes = useStyles();
  let history = useHistory();
  function variantChangeRedirect(e, data) {
    setVariant(name, data.variant);
    history.push(history.location.pathname + "/" + name);
  }
  let myVariants = [];
  variants.forEach((variant) => {
    myVariants.push({
      variant: variant,
      description: mutableRes.find((el) => el.name == name).variants[variant]
        .description,
    });
  });
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
          {
            title: "Usage",
            field: "usage",
            render: (row) => <UsageTab />,
          },
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
        <Typography variant="h4">No {type}s Registered</Typography>
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
