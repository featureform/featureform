// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Grid from '@mui/material/Grid';
import Tab from '@mui/material/Tab';
import Tabs from '@mui/material/Tabs';
import Typography from '@mui/material/Typography';
import { styled } from '@mui/system';
import PropTypes from 'prop-types';
import React, { useState } from 'react';
import FeatureDistribution from '../../graphs/FeatureDistribution';
import FeatureSetList from '../../lists/FeatureSetList';

function TabPanel(props) {
  const { children, value, index } = props;

  return (
    <div
      role='tabpanel'
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
    >
      {value === index && (
        <Box p={3}>
          <Typography>{children}</Typography>
        </Box>
      )}
    </div>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.any.isRequired,
  value: PropTypes.any.isRequired,
};

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    'aria-controls': `simple-tabpanel-${index}`,
  };
}

const RootBox = styled('div')(({ theme }) => ({
  flexGrow: 1,
  backgroundColor: theme.palette.background.paper,
}));

const DetailBox = styled('div')(({ theme }) => ({
  margin: theme.spacing(1),
}));

const StyledTabs = styled(Tabs)(({ theme }) => ({
  borderRight: `1px solid ${theme.palette.divider}`,
}));

const FeatureDataView = ({ data }) => {
  const [value, setValue] = useState(0);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  return (
    <RootBox>
      <AppBar position='static'>
        <StyledTabs
          value={value}
          onChange={handleChange}
          aria-label='simple tabs example'
        >
          <Tab label='Feature Data' {...a11yProps(0)} />
          <Tab label='Statistics' {...a11yProps(1)} />
          <Tab label='Feature Sets' {...a11yProps(2)} />
        </StyledTabs>
      </AppBar>
      <TabPanel value={value} index={0}>
        <Grid container item justifyContent='center' direction='row' lg={12}>
          <DetailBox>
            <p>
              <b>Version name: </b> {data['version-name']}
            </p>
            <p>
              <b>Source: </b> {data['source']}
            </p>
            <p>
              <b>Entity: </b> {data['entity']}
            </p>
          </DetailBox>
          <DetailBox>
            <p>
              <b>Created: </b> {data['created']}
            </p>
            <p>
              <b>Last Updated: </b> {data['last-updated']}
            </p>
          </DetailBox>
          <DetailBox>
            <p>
              <b>Owner: </b> {data['owner']}
            </p>
            <p>
              <b>Visibility: </b> {data['visibility']}
            </p>
            <p>
              <b>Revision: </b> {data['revision']}
            </p>
          </DetailBox>
          <DetailBox>
            <p>
              <b>Description: </b> {data['description']}
            </p>
          </DetailBox>
        </Grid>
      </TabPanel>
      <TabPanel value={value} index={1}>
        <DetailBox>
          <p>
            <b>Statistical Data: </b> {JSON.stringify(data['statistical data'])}
          </p>
          <FeatureDistribution
            data={data['statistical data']['distribution-data']}
          />
        </DetailBox>
      </TabPanel>
      <TabPanel value={value} index={2}>
        <DetailBox>
          <FeatureSetList data={data['feature sets']} />
        </DetailBox>
      </TabPanel>
    </RootBox>
  );
};

FeatureDataView.propTypes = {
  data: PropTypes.object.isRequired,
};

export default FeatureDataView;
