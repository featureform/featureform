import React from "react";
import PropTypes from "prop-types";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import Box from "@material-ui/core/Box";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";
import Container from "@material-ui/core/Container";
import Resource from "../../api/resources/Resource.js";
import { useRouter } from "next/router"

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
            {results?.length > 0 ? (
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
  let filteredContentHits = {};
  let moreFilteredContents = [];
  if (contents?.length) {
    moreFilteredContents = contents.filter((content) => {
      if (content.Name + "." + content.Variant + "." + content.Type in filteredContentHits) {
        return false;
      }
      filteredContentHits[content.Name + "." + content.Variant + "." + content.Type] = content.Variant;
      return true;
    }); 
  }
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

const searchTypeMap = Object.freeze({
  "FEATURE": "Feature",
  "FEATURE_VARIANT": "Feature",
  "LABEL": "Label",
  "LABEL_VARIANT": "Label",
  "TRAINING_SET": "TrainingSet",
  "TRAINING_SET_VARIANT": "TrainingSet",
  "SOURCE": "Source",
  "SOURCE_VARIANT": "Source",
  "PROVIDER": "Provider",
  "ENTITY": "Entity",
  "MODEL": "Model",
  "USER": "User",
})

const SearchResultsItem = ({ type, content, setVariant }) => {
  const classes = useStyles();
  const router = useRouter()
  const resourceType = Resource[searchTypeMap[content.Type?.toUpperCase()]];
  function handleClick(content) {
    if (resourceType?.hasVariants) {
      setVariant(content.Type, content.Name, content.Variant);
    }
    router.push(resourceType.urlPathResource(content.Name));
  }

  return (
      <>
        <ListItem button alignItems="flex-start" onClick={() => handleClick(content)} >
          <ListItemText
              primary={
                <div>
                  <div>
                    <div className={classes.resultTitle}>
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
          />
        </ListItem>
      </>
  );
};

export default SearchResultsView;