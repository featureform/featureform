// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Chip from '@mui/material/Chip';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import { styled } from '@mui/system';
import React from 'react';

const AttributeContainer = styled(Container)(({ theme }) => ({
  padding: theme.spacing(2),
  borderRadius: '16px',
  border: `1px solid ${theme.palette.border.main}`,
}));

const StyledChip = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.5),
}));

/**
 *
 * @param {attributes} param0 - ([]string) A list attributes string (e.g. resource tags)
 * @param {title} param1 - (string) The attributes' title (e.g. "Tags")
 * @returns {Container}
 */
const AttributeBox = ({ attributes, title }) => {
  return (
    <AttributeContainer>
      <Typography variant='h6' component='h5' gutterBottom>
        {title}
      </Typography>
      {attributes ? (
        attributes.map((attr) => (
          <StyledChip label={attr} key={attr} variant='outlined' />
        ))
      ) : (
        <div></div>
      )}
    </AttributeContainer>
  );
};

export default AttributeBox;
