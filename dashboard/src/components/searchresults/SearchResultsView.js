import Box from '@mui/material/Box';
import Container from '@mui/material/Container';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemText from '@mui/material/ListItemText';
import Typography from '@mui/material/Typography';
import { makeStyles } from '@mui/styles';
import { useRouter } from 'next/router';
import PropTypes from 'prop-types';
import React from 'react';
import Resource from '../../api/resources/Resource.js';

function TabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role='tabpanel'
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
    background: 'rgba(255, 255, 255, 1)',
    border: `2px solid ${theme.palette.border.main}`,
  },

  appbar: {
    background: 'transparent',
    boxShadow: 'none',
    color: 'black',
    padding: theme.spacing(0),
    paddingLeft: theme.spacing(4),
    paddingRight: theme.spacing(4),
  },

  searchTitle: {
    padding: theme.spacing(1),
    paddingTop: theme.spacing(4),
  },

  resultTitle: {
    display: 'inline',
    lineHeight: 1.2,
  },
}));

const SearchResultsView = ({ results, search_query, setVariant }) => {
  const classes = useStyles();
  return (
    <div>
      <Container maxWidth='xl' className={classes.root}>
        <Typography
          className={classes.searchTitle}
          variant='h4'
          style={{ display: 'flex' }}
        >
          {results?.length > 0 ? (
            <span style={{ color: 'gray' }}>
              {results?.length} {results.length < 2 ? 'result' : 'results'}{' '}
              for:&nbsp;
            </span>
          ) : (
            <span style={{ color: 'gray' }}>No results for:&nbsp;</span>
          )}
          <b>{search_query}</b>
        </Typography>
        {results?.length > 0 ? (
          <SearchResultsList contents={results} setVariant={setVariant} />
        ) : null}
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
      if (
        content.Name + '.' + content.Variant + '.' + content.Type in
        filteredContentHits
      ) {
        return false;
      }
      filteredContentHits[
        content.Name + '.' + content.Variant + '.' + content.Type
      ] = content.Variant;
      return true;
    });
  }
  return (
    <div style={{ marginBottom: '2em' }}>
      <List className={classes.root} component='nav'>
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
  FEATURE: 'Feature',
  FEATURE_VARIANT: 'Feature',
  LABEL: 'Label',
  LABEL_VARIANT: 'Label',
  TRAINING_SET: 'TrainingSet',
  TRAINING_SET_VARIANT: 'TrainingSet',
  SOURCE: 'Source',
  SOURCE_VARIANT: 'Source',
  PROVIDER: 'Provider',
  ENTITY: 'Entity',
  MODEL: 'Model',
  USER: 'User',
});

const SearchResultsItem = ({ content, setVariant }) => {
  const classes = useStyles();
  const router = useRouter();
  const resourceType = Resource[searchTypeMap[content.Type?.toUpperCase()]];

  function handleClick(content) {
    if (resourceType?.hasVariants) {
      setVariant(resourceType.type, content.Name, content.Variant);
      const base = resourceType.urlPathResource(content.Name);
      router.push(`${base}?variant=${content.Variant}`);
    } else {
      router.push(resourceType.urlPathResource(content.Name));
    }
  }

  return (
    <>
      <ListItem
        button
        alignItems='flex-start'
        onClick={() => handleClick(content)}
      >
        <ListItemText
          primary={
            <div>
              <div>
                <div className={classes.resultTitle}></div>
                <Typography className={classes.resultTitle} variant='h6'>
                  {content.Name}
                </Typography>{' '}
              </div>
              <div style={{ width: '0.5em' }}>{'   '}</div>
              <Typography
                style={{ opacity: 0.5 }}
                className={classes.resultTitle}
                variant='body1'
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
