import AddBoxOutlinedIcon from '@mui/icons-material/AddBoxOutlined';
import RemoveOutlinedIcon from '@mui/icons-material/RemoveOutlined';
import { Button, TextField } from '@mui/material';
import Chip from '@mui/material/Chip';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import { makeStyles } from '@mui/styles';
import React from 'react';
import { useDataAPI } from '../../../hooks/dataAPI';

const useStyles = makeStyles((theme) => ({
  formControl: {
    margin: theme.spacing(1),
    minWidth: 120,
  },
  selectEmpty: {
    marginTop: theme.spacing(2),
  },
  attributeContainer: {
    padding: theme.spacing(2),
    borderRadius: '16px',
    border: `1px solid ${theme.palette.border.main}`,
  },
  chip: {
    margin: theme.spacing(0.5),
  },
}));

const TagBox = ({
  type = '',
  resourceName = '',
  variant = '',
  tags = [],
  title = '',
}) => {
  const ENTER_KEY = 'Enter';
  const classes = useStyles();
  const [tagName, setTagName] = React.useState('');
  const [tagList, setTagsList] = React.useState(tags);
  const [displayTextOpen, setDisplayTextOpen] = React.useState(false);

  const dataAPI = useDataAPI();
  async function handleNewTag(event) {
    event.preventDefault();
    if (tagName?.trim()) {
      let updatedList = [...tagList.filter((t) => t != tagName), tagName];
      let data = await dataAPI.postTags(
        type,
        resourceName,
        variant,
        updatedList
      );
      if (data?.tags) {
        setTagsList(data.tags);
      }
    }
  }

  async function handleDeleteTag(deleteTag = '') {
    let updatedList = tagList.filter((tagName) => tagName != deleteTag);
    let data = await dataAPI.postTags(type, resourceName, variant, updatedList);
    if (data?.tags) {
      setTagsList(data.tags);
    }
  }

  const toggleOpenText = () => {
    setDisplayTextOpen(!displayTextOpen);
    setTagName('');
  };

  return (
    <Container className={classes.attributeContainer}>
      <Typography variant='h6' component='h5' gutterBottom>
        {title}
        <Button
          size='small'
          variant='text'
          data-testid='displayTextBtnId'
          onClick={toggleOpenText}
        >
          {displayTextOpen ? <RemoveOutlinedIcon /> : <AddBoxOutlinedIcon />}
        </Button>
      </Typography>
      {displayTextOpen ? (
        <>
          <TextField
            label='New Tag'
            variant='standard'
            onChange={(event) => {
              const rawText = event.target.value;
              if (rawText === '') {
                setTagName(rawText);
                return;
              }
              const tagName = event.target.value ?? '';
              if (tagName.trim()) {
                setTagName(tagName.trim());
              }
            }}
            value={tagName}
            onKeyDown={(event) => {
              if (event.key === ENTER_KEY && setTagName) {
                handleNewTag(event);
                setTagName('');
              }
            }}
            inputProps={{
              'aria-label': 'search',
              'data-testid': 'tagInputId',
            }}
          />
          <br />
        </>
      ) : null}
      {tagList.map((tag) => (
        <Chip
          label={tag}
          key={tag}
          data-testid={tag + 'id'}
          className={classes.chip}
          style={{ marginTop: '10px' }}
          variant='outlined'
          onDelete={() => handleDeleteTag(tag)}
        />
      ))}
    </Container>
  );
};

export default TagBox;
