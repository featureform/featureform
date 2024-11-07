// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import { styled } from '@mui/system';
import { useRouter } from 'next/router';
import React from 'react';
import { connect } from 'react-redux';
import Resource from '../../api/resources/Resource';
import { WHITE } from '../../styles/theme';
import SideNavHeader from './SideNavHeader';
import SideNavItem from './SideNavItem';

const drawerWidth = 210;
const drawerTop = 71;

const PREFIX = 'SideNav';

const classes = {
  drawer: `${PREFIX}-drawer`,
  navBox: `${PREFIX}-navBox`,
  mainContent: `${PREFIX}-mainContent`,
  listItemHome: `${PREFIX}-listItemHome`,
  listItem: `${PREFIX}-listItem`,
  listIcon: `${PREFIX}-listIcon`,
  listIconImg: `${PREFIX}-listIconImg`,
  listHeader: `${PREFIX}-listHeader`,
  listSvg: `${PREFIX}-listSvg`,
};

const Root = styled('div')(() => ({
  [`& .${classes.drawer}`]: {
    width: drawerWidth,
    flexShrink: 0,
    '& .MuiDrawer-paper': {
      background: WHITE,
      width: drawerWidth,
      boxSizing: 'border-box',
      top: `${drawerTop}px`,
      borderTop: '1px solid #e1e2e5',
    },
  },
  [`& .${classes.navBox}`]: {
    display: 'flex',
  },
  [`& .${classes.mainContent}`]: {
    flexGrow: 1,
    p: 3,
  },
  [`& .${classes.listItemHome}`]: {
    padding: '0 0 0 0',
    color: '#B3B5DC',
    backgroundColor: '#111014',
  },
  [`& .${classes.listItem}`]: {
    padding: '0 0 0 0',
    marginBottom: -5,
    color: '#B3B5DC',
  },
  [`& .${classes.listIcon}`]: {
    minWidth: 32,
  },
  [`& .${classes.listIconImg}`]: {
    marginBottom: 5,
  },
  [`& .${classes.listHeader}`]: {
    marginTop: 15,
    marginBottom: 0,
    display: 'flex',
    marginLeft: 15,
    color: '#4F5174',
  },
  [`& .${classes.listSvg}`]: {
    width: '50%',
  },
}));

export { classes };

function SideNav({ sections }) {
  const router = useRouter();
  function handleMenuClick(event, urlPath) {
    event.preventDefault();
    router.push(urlPath);
  }

  return (
    <>
      <Root>
        <Box className={classes.navBox}>
          <CssBaseline />
          <Drawer variant='permanent' className={classes.drawer}>
            <List>
              <SideNavItem
                handler={handleMenuClick}
                iconType={'home'}
                urlPath={'/'}
                currentPath={router?.asPath}
                displayText={'Home'}
              />
              <SideNavHeader displayText={'Resources'} />
              {sections?.['features'].map((currentSection, index) => {
                let resourceType = Resource[currentSection.type];
                return (
                  <SideNavItem
                    key={index + 100}
                    handler={handleMenuClick}
                    iconType={resourceType.materialIcon}
                    urlPath={resourceType.urlPath}
                    currentPath={router?.asPath}
                    displayText={resourceType.typePlural}
                  />
                );
              })}
              <SideNavHeader displayText={'Scheduling'} />
              <SideNavItem
                handler={handleMenuClick}
                iconType='tasks'
                urlPath={'/tasks'}
                currentPath={router?.asPath}
                displayText={'Tasks'}
              />
            </List>
          </Drawer>
        </Box>
      </Root>
    </>
  );
}

const mapStateToProps = (state) => ({
  sections: state.homePageSections,
});

export default connect(mapStateToProps)(SideNav);
