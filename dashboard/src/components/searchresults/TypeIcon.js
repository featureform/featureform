// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { Box } from '@mui/material';
import { styled } from '@mui/system';
import React from 'react';
import { DataSetSvg } from '../../components/sideNav/Icons/Datasets';
import { EntitiesSvg } from '../../components/sideNav/Icons/Entities';
import { FeatureSvg } from '../../components/sideNav/Icons/Features';
import { LabelsSvg } from '../../components/sideNav/Icons/Labels';
import { ModelsSvg } from '../../components/sideNav/Icons/Models';
import { ProvidersSvg } from '../../components/sideNav/Icons/Providers';
import { TrainingSetsSvg } from '../../components/sideNav/Icons/TrainingSets';

const DEFAULT_FILL = '#B3B5DC';
const DEFAULT_KEY = 'default';

const ICON_MAP = Object.freeze({
  TRAINING_SET: { svg: TrainingSetsSvg, selected: '#DA1E28' },
  TRAINING_SET_VARIANT: { svg: TrainingSetsSvg, selected: '#DA1E28' },
  FEATURE: { svg: FeatureSvg, selected: '#EF4444' },
  FEATURE_VARIANT: { svg: FeatureSvg, selected: '#EF4444' },
  USER: { svg: EntitiesSvg, selected: '#F59E0B' },
  ENTITY: { svg: EntitiesSvg, selected: '#F59E0B' },
  LABEL: { svg: LabelsSvg, selected: '#F97316' },
  LABEL_VARIANT: { svg: LabelsSvg, selected: '#F97316' },
  MODEL: { svg: ModelsSvg, selected: '#EAB308' },
  SOURCE: { svg: DataSetSvg, selected: '#F2BB51' },
  SOURCE_VARIANT: { svg: DataSetSvg, selected: '#F2BB51' },
  PROVIDER: { svg: ProvidersSvg, selected: '#8B5CF6' },
  default: { svg: FeatureSvg, selected: DEFAULT_FILL },
});

const Container = styled(Box)({
  marginBottom: 1,
});

export default function TypeIcon({ iconKey = '' }) {
  const item = iconKey in ICON_MAP ? ICON_MAP[iconKey] : ICON_MAP[DEFAULT_KEY];
  const SVG = item.svg;
  const selectedFill = item.selected;

  return (
    <Container>
      <SVG
        data-testid={'svgId-' + iconKey}
        fill={selectedFill}
        height='20'
        width='20'
        style={{ minWidth: 30 }}
      />
    </Container>
  );
}
