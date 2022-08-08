import React from "react";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";
import Container from "@material-ui/core/Container";
import Icon from "@material-ui/core/Icon";
import Resource from "../../api/resources/Resource.js";

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
    display: "inline",
    lineHeight: 1.2,
  },
}));

//resolution between proto labeling of different types (resolves them to unicode values) and the actual names of the resource types
var searchTypeMap = {
  '\u0004': "Feature", //feature variant
  '\u0005': "Label",
  '\u0006': "TrainingSet",
  '\u0007': "Source",
  '\u0008': "Provider",
  '\u0009': "Entity",
  '\u000a': "Model",
  '\u000b': "User",
}

const SearchResultsView = ({ results, search_query, setVariant }) => {
  const classes = useStyles();
  return (
    <div>
      <Container maxWidth="xl" className={classes.root}>
        <Typography
          className={classes.searchTitle}
          variant="h4"
          style={{ display: "flex" }}
        >
          {results.length > 0 ? (
            <div style={{ color: "gray" }}>Results for:&nbsp;</div>
          ) : (
            <div style={{ color: "gray" }}>No results for:&nbsp;</div>
          )}

          <b>{search_query}</b>
        </Typography>
        <SearchResultsList contents={results} setVariant={setVariant} />
      </Container>
    </div>
  );
};

const SearchResultsList = ({ type, contents, setVariant }) => {
  const classes = useStyles();
  let filteredContents = contents.filter(
    (content) => searchTypeMap[content.Type]
  );
  let filteredContentHits = {};
  let moreFilteredContents = filteredContents.filter((content) => {
    if (content.Name + "." + content.Variant + "." + searchTypeMap[content.Type] in filteredContentHits) {
      return false;
    }
    filteredContentHits[content.Name + "." + content.Variant + "." + searchTypeMap[content.Type]] = content.Variant;
    return true;
  });
  return (
    <div>
      <List className={classes.root} component="nav">
        {moreFilteredContents.map((content, i) => (
          <SearchResultsItem
            key={i}
            type={type}
            content={content}
            setVariant={setVariant}
          />
        ))}
      </List>
    </div>
  );
};

const SearchResultsItem = ({ type, content, setVariant }) => {
  const classes = useStyles();
  let history = useHistory();

  const resourceType = Resource[searchTypeMap[content.Type]];
  const resourceIcon = resourceType.materialIcon;
  function handleClick(content) {
    if (resourceType.hasVariants) {
      setVariant(searchTypeMap[content.Type], content.Name, content.Variant);
    }
    history.push(resourceType.urlPathResource(content.Name));
  }

  return (
    <div>
      <ListItem button alignItems="flex-start">
        <ListItemText
          primary={
            <div>
              <div>
                <div className={classes.resultTitle}>
                  <Icon>{resourceIcon}</Icon>
                </div>
                <Typography className={classes.resultTitle} variant="h6">
                  {content.Name}
                </Typography>{" "}
              </div>
              <div style={{ width: "0.5em" }}>{"   "}</div>
              <Typography
                style={{ opacity: 0.5 }}
                className={classes.resultTitle}
                variant="body1"
              >
                {content.Variant}
              </Typography>
            </div>
          }
          onClick={() => handleClick(content)}
        />
      </ListItem>
    </div>
  );
};

export default SearchResultsView;
