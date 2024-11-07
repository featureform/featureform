// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import AddBoxOutlinedIcon from '@mui/icons-material/AddBoxOutlined';
import RemoveOutlinedIcon from '@mui/icons-material/RemoveOutlined';
import {
  Box,
  Button,
  Chip,
  Container,
  TextField,
  Typography,
} from '@mui/material';
import { styled } from '@mui/system';
import React, { useEffect, useRef, useState } from 'react';
import { useDataAPI } from '../../../hooks/dataAPI';

const StyledContainer = styled(Container)(({ theme }) => ({
  margin: theme.spacing(1),
  minWidth: 120,
}));

const TagTopRow = styled(Box)(() => ({
  minHeight: 60,
  minWidth: 300,
  display: 'flex',
}));

const StyledChip = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.5),
}));

const TagBox = ({
  type = '',
  resourceName = '',
  variant = '',
  tags = [],
  title = '',
}) => {
  const ENTER_KEY = 'Enter';
  const [tagName, setTagName] = useState('');
  const [tagList, setTagsList] = useState(tags);
  const [displayTextOpen, setDisplayTextOpen] = useState(false);
  const ref = useRef();

  const dataAPI = useDataAPI();

  async function handleNewTag(event) {
    event.preventDefault();
    if (tagName?.trim()) {
      let updatedList = [...tagList.filter((t) => t !== tagName), tagName];
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
    let updatedList = tagList.filter((tagName) => tagName !== deleteTag);
    let data = await dataAPI.postTags(type, resourceName, variant, updatedList);
    if (data?.tags) {
      setTagsList(data.tags);
    }
  }

  const toggleOpenText = () => {
    setDisplayTextOpen(!displayTextOpen);
    setTagName('');
  };

  useEffect(() => {
    async function fetchData() {
      let data = await dataAPI.getTags(type, resourceName, variant);
      let localTags = [...tagList];
      if (
        data?.tags &&
        data.tags.sort().toString() !== localTags?.sort().toString()
      ) {
        if (ref?.current) {
          setTagsList(data.tags);
        }
      }
    }
    fetchData();
  }, [variant]);

  return (
    <StyledContainer ref={ref}>
      <TagTopRow>
        <Typography
          variant='h6'
          component='h5'
          gutterBottom
          sx={{ paddingTop: 2 }}
        >
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
        {displayTextOpen && (
          <TextField
            label='New Tag'
            sx={{ maxWidth: 160 }}
            autoFocus
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
              if (event.key === ENTER_KEY && tagName) {
                handleNewTag(event);
                setTagName('');
              }
            }}
            inputProps={{
              'aria-label': 'search',
              'data-testid': 'tagInputId',
            }}
          />
        )}
      </TagTopRow>

      <Box>
        {tagList.map((tag) => (
          <StyledChip
            label={tag}
            key={tag}
            data-testid={tag + 'id'}
            sx={{ marginTop: 1 }}
            variant='outlined'
            onDelete={() => handleDeleteTag(tag)}
          />
        ))}
      </Box>
    </StyledContainer>
  );
};

export default TagBox;
