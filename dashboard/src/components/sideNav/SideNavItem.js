// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemText from '@mui/material/ListItemText';
import { styled } from '@mui/system';
import React, { useState } from 'react';
import { FEATUREFORM_DARK_GRAY, WHITE } from '../../styles/theme';
import SideNavIcon from './SideNavIcon';

const ListItemHome = styled(ListItem)(() => ({
  padding: '0 0 0 0',
  color: FEATUREFORM_DARK_GRAY,
  backgroundColor: WHITE,
}));

const ListItemCustom = styled(ListItem)(() => ({
  padding: '0 0 0 0',
  marginBottom: -5,
  color: FEATUREFORM_DARK_GRAY,
}));

export default function SideNavItem({
  iconType = '',
  displayText = '',
  urlPath = '',
  currentPath = '',
  handler = () => null,
}) {
  const [isHovered, setIsHovered] = useState(false);

  const shouldSelect = () => {
    let result = false;
    if (urlPath === currentPath && currentPath === '/') {
      // Check for home page only
      result = true;
    } else if (
      (urlPath !== '/' && currentPath.startsWith(urlPath)) ||
      isHovered
    ) {
      // Check for non-homepage route, and if hovered
      result = true;
    }
    return result;
  };

  const ListItemComponent = iconType === 'home' ? ListItemHome : ListItemCustom;

  const StyledListItemButton = styled(ListItemButton)(() => ({
    backgroundColor: shouldSelect() ? '#F9F9FA' : 'transparent',
    padding: '10px 20px',
  }));

  return (
    <ListItemComponent
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <StyledListItemButton onClick={(event) => handler(event, urlPath)}>
        <SideNavIcon iconKey={iconType} isSelected={shouldSelect()} />
        <ListItemText primary={displayText} />
      </StyledListItemButton>
    </ListItemComponent>
  );
}
