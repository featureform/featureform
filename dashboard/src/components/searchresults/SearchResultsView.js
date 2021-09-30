import React, { useEffect } from "react";
import AppBar from "@material-ui/core/AppBar";
import Tabs from "@material-ui/core/Tabs";
import Tab from "@material-ui/core/Tab";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import { useHistory } from "react-router-dom";
import Paper from "@material-ui/core/Paper";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import Divider from "@material-ui/core/Divider";
import ListItemText from "@material-ui/core/ListItemText";
import { resourcePaths } from "api/resources";
import Icon from "@material-ui/core/Icon";
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

  searchTitle: {
    padding: theme.spacing(2),
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

const SearchResultsView = ({ results, search_query }) => {
  const classes = useStyles();

  const [value, setValue] = React.useState(0);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  useEffect(() => {
    setValue(0);
  }, [search_query]);

  return !results.loading && !results.failed && results.resources ? (
    <div>
      <Paper elevation={3}>
        <Typography className={classes.searchTitle} variant="h5">
          Results for: <b>{search_query}</b>
        </Typography>
        <AppBar position="static">
          <Tabs
            value={value}
            onChange={handleChange}
            aria-label="simple tabs example"
          >
            {results.resources.typeOrder.map((type, i) => (
              <Tab label={type} {...a11yProps(i)} />
            ))}
          </Tabs>
        </AppBar>

        {results.resources.typeOrder.map((type, i) => (
          <TabPanel value={value} index={i}>
            <SearchResultsList
              type={type}
              contents={results.resources.data[type]}
            />
          </TabPanel>
        ))}
      </Paper>
    </div>
  ) : (
    <div></div>
  );
};

const SearchResultsList = ({ type, contents }) => {
  const classes = useStyles();

  return (
    <div>
      <List className={classes.root} component="nav">
        {contents.map((content) => (
          <SearchResultsItem type={type} content={content} />
        ))}

        <Divider component="li" />
      </List>
    </div>
  );
};

const SearchResultsItem = ({ type, content }) => {
  const history = useHistory();
  const classes = useStyles();

  function handleClick(event) {
    history.push(resourcePaths[type] + "/" + content.name);
  }

  const name = content.formattedName;
  const description = content.formattedDescription;
  return (
    <ListItem button alignItems="flex-start">
      <ListItemText
        primary={
          <div>
            <Icon className={classes.resultTitle}>{resourceIcons[type]}</Icon>
            <div className={classes.resultTitle}>&nbsp;</div>
            <Typography
              className={classes.resultTitle}
              variant="h6"
              dangerouslySetInnerHTML={{ __html: name }}
            />
          </div>
        }
        onClick={handleClick}
        secondary={
          <React.Fragment>
            <span dangerouslySetInnerHTML={{ __html: description }} />
          </React.Fragment>
        }
      />
    </ListItem>
  );
};

export default SearchResultsView;
