import React, { useEffect } from "react";
import AppBar from "@material-ui/core/AppBar";
import Tabs from "@material-ui/core/Tabs";
import Tab from "@material-ui/core/Tab";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import { useHistory } from "react-router-dom";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import ListSubheader from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";
import { resourcePaths } from "api/resources";
import Icon from "@material-ui/core/Icon";
import { resourceIcons } from "api/resources";
import Container from "@material-ui/core/Container";
import { setVersion } from "components/resource-list/VersionSlice";

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
    borderRadius: 16,
    background: "rgba(255, 255, 255, 1)",
    border: `2px solid ${theme.palette.border.main}`,
  },

  appbar: {
    background: "transparent",
    boxShadow: "none",
    color: "black",
    padding: theme.spacing(0),
    paddingLeft: theme.spacing(4),
    paddingRight: theme.spacing(4),
  },

  searchTitle: {
    padding: theme.spacing(1),
    paddingTop: theme.spacing(4),
  },

  resultTitle: {
    display: "inline-block",
    lineHeight: 1.2,
  },
}));

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    "aria-controls": `simple-tabpanel-${index}`,
  };
}

const SearchResultsView = ({ results, search_query, setVersion }) => {
  const classes = useStyles();

  const [value, setValue] = React.useState(0);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  let myResults = results.resources;
  let typeOrder = [
    "Feature",
    "Data Source",
    "Entity",
    "Model",
    "Transformation",
    "Training Dataset",
  ];

  useEffect(() => {
    setValue(0);
  }, [search_query]);

  return !results.loading && !results.failed && results.resources ? (
    <div>
      <Container maxWidth="xl" className={classes.root}>
        <Typography
          className={classes.searchTitle}
          variant="h4"
          style={{ display: "flex" }}
        >
          <div style={{ color: "gray" }}>Results for:&nbsp;</div>

          <b>{search_query}</b>
        </Typography>
        <AppBar position="static" className={classes.appbar}>
          <Tabs
            value={value}
            onChange={handleChange}
            aria-label="simple tabs example"
          >
            {typeOrder.map((type, i) => (
              <Tab label={type} {...a11yProps(i)} />
            ))}
          </Tabs>
        </AppBar>

        {typeOrder.map((type, i) => (
          <TabPanel value={value} index={i}>
            <SearchResultsList
              type={type}
              contents={myResults[type]}
              setVersion={setVersion}
            />
          </TabPanel>
        ))}
      </Container>
    </div>
  ) : (
    <div></div>
  );
};

const SearchResultsList = ({ type, contents, setVersion }) => {
  const classes = useStyles();

  return (
    <div>
      <List className={classes.root} component="nav">
        {contents.map((content) => (
          <SearchResultsItem
            type={type}
            content={content}
            setVersion={setVersion}
          />
        ))}
      </List>
    </div>
  );
};

const SearchResultsItem = ({ type, content, setVersion }) => {
  const history = useHistory();
  const classes = useStyles();

  function handleClick(variant) {
    setVersion(type, content.name, variant);
    history.push(resourcePaths[type] + "/" + content.name);
  }

  const name = content.formattedName;
  const description = content.formattedDescription;
  return (
    <>
      <ListSubheader>
        <Icon className={classes.resultTitle}>{resourceIcons[type]}</Icon>
        <div className={classes.resultTitle}>&nbsp;</div>
        <Typography
          className={classes.resultTitle}
          variant="body1"
          dangerouslySetInnerHTML={{ __html: content.name }}
        ></Typography>
      </ListSubheader>

      {Object.keys(content.versions).map((variant, i) => (
        <ListItem button alignItems="flex-start">
          <ListItemText
            inset
            primary={
              <Typography variant="body1">
                <b>{variant}</b>
              </Typography>
            }
            onClick={() => handleClick(variant)}
            secondary={content.versions[variant].description}
          />
        </ListItem>
      ))}
      {/*  */}
    </>
  );
};

export default SearchResultsView;
