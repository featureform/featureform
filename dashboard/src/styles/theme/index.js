// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { blueGrey } from '@mui/material/colors';
import { createTheme, responsiveFontSizes } from '@mui/material/styles';

export const GREEN = '#27ae60';
export const WHITE = '#FFFFFF';
export const FEATUREFORM_PURPLE = '#7A14E5';
export const FEATUREFORM_DARK_PURPLE = '#3D0A73';
export const FEATUREFORM_LIGHT_PURPLE = '#6a32a6';
export const FEATUREFORM_RED = '#f7195c';
export const FEATUREFORM_LIGHT_RED = '#F9477D';
export const FEATUREFORM_LIGHT_GRAY = '#F5F6F7';
export const FEATUREFORM_DARK_GRAY = '#6c6e72';
export const FEATUREFORM_LIGHT_BLACK = '#061B3F';
export const FEATUREFORM_RED_BROWN = '#7C0D2E';

const themeSpec = {
  overrides: {
    MuiCssBaseline: {
      '@global': {
        body: {
          background: WHITE,
          backgroundRepeat: 'no-repeat',
          backgroundAttachment: 'fixed',
        },
      },
    },
  },
  palette: {
    background: {
      default: WHITE,
    },
    primary: { main: FEATUREFORM_RED, contrastText: WHITE },
    secondary: { main: FEATUREFORM_RED },
    error: { main: FEATUREFORM_RED },
    border: { main: FEATUREFORM_LIGHT_GRAY, alternate: FEATUREFORM_DARK_GRAY },
    text: {
      primary: blueGrey[900],
      secondary: blueGrey[700],
    },
    ordinalColors: {
      0: FEATUREFORM_DARK_PURPLE,
      1: FEATUREFORM_RED,
      2: FEATUREFORM_LIGHT_PURPLE,
      3: FEATUREFORM_LIGHT_RED,
    },
    hoverColors: {
      0: '#CBC3E3',
      1: '#FFCCCB',
    },
  },
  typography: {
    fontFamily: `"Matter", "Lato", "Helvetica", sans-serif`,
    fontSize: 14,
    fontWeightLight: 450,
    fontWeightRegular: 550,
    fontWeightMedium: 650,
  },
};

let theme = createTheme(themeSpec);
theme = responsiveFontSizes(theme);

export default theme;
