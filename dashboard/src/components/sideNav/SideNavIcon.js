// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import ListItemIcon from '@mui/material/ListItemIcon';
import { styled } from '@mui/system';
import React from 'react';
import { FEATUREFORM_DARK_GRAY } from '../../styles/theme';
import { DataSetSvg } from './Icons/Datasets';
import { EntitiesSvg } from './Icons/Entities';
import { FeatureSvg } from './Icons/Features';
import { GroupsSvg } from './Icons/Groups';
import { HomeSvg } from './Icons/Home';
import { LabelsSvg } from './Icons/Labels';
import { LogOutSvg } from './Icons/LogOut';
import { ModelsSvg } from './Icons/Models';
import { ProvidersSvg } from './Icons/Providers';
import { RolesSvg } from './Icons/Roles';
import { TasksSvg } from './Icons/Tasks';
import { TrainingSetsSvg } from './Icons/TrainingSets';
import { UsersSvg } from './Icons/Users';

const ListIconStyled = styled(ListItemIcon)({
  minWidth: 32,
});

const DEFAULT_FILL = FEATUREFORM_DARK_GRAY;
const DEFAULT_KEY = 'default';

const ICON_MAP = Object.freeze({
  home: { svg: HomeSvg, selected: '#ED215E' },
  storage: { svg: TrainingSetsSvg, selected: '#DA1E28' },
  description: { svg: FeatureSvg, selected: '#EF4444' },
  fingerprint: { svg: EntitiesSvg, selected: '#F59E0B' },
  label: { svg: LabelsSvg, selected: '#F97316' },
  model_training: { svg: ModelsSvg, selected: '#EAB308' },
  source: { svg: DataSetSvg, selected: '#F2BB51' },
  device_hub: { svg: ProvidersSvg, selected: '#8B5CF6' },
  tasks: { svg: TasksSvg, selected: '#4ADE80' },
  person: { svg: UsersSvg, selected: '#22D3EE' },
  engineer: { svg: RolesSvg, selected: '#0EA5E9' },
  group: { svg: GroupsSvg, selected: '#3B82F6' },
  exit: { svg: LogOutSvg, selected: DEFAULT_FILL },
  default: { svg: FeatureSvg, selected: DEFAULT_FILL },
});

export default function SideNavIcon({ iconKey = '', isSelected = false }) {
  const item = iconKey in ICON_MAP ? ICON_MAP[iconKey] : ICON_MAP[DEFAULT_KEY];
  const SVG = item.svg;
  const selectedFill = item.selected;

  return (
    <ListIconStyled>
      <SVG
        data-testid={'svgId-' + iconKey}
        fill={isSelected ? selectedFill : DEFAULT_FILL}
        height='20'
        width='20'
      />
    </ListIconStyled>
  );
}
