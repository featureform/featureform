// # This Source Code Form is subject to the terms of the Mozilla Public
// # License, v. 2.0. If a copy of the MPL was not distributed with this
// # file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// # Copyright 2024 FeatureForm Inc.
//

import { AppBar, Box, Toolbar } from '@mui/material';
import { styled } from '@mui/system';
import { useRouter } from 'next/router';
import React, { useEffect, useState } from 'react';
import { WHITE } from '../../styles/theme';
import SearchBar from '../search/SearchBar';

const CustomAppBar = styled(AppBar)({
  position: 'fixed',
  top: 0,
  width: '100%',
  height: '72px',
  left: 0,
  background: WHITE,
  boxShadow: 'none',
  borderBottom: `1px solid #e1e2e5`,
});

const InstanceLogo = styled('img')({
  height: '3em',
  cursor: 'pointer',
});

const CustomToolbar = styled(Toolbar)({
  width: '100%',
  display: 'flex',
  justifyContent: 'space-between',
  color: '#B3B5DC',
  boxShadow: 'none',
});

const TitleBox = styled(Box)(({ theme }) => ({
  justifySelf: 'flex-start',
  paddingLeft: theme.spacing(1.5),
}));

const ToolbarRight = styled(Box)({
  alignItems: 'center',
  display: 'flex',
});

const ToolbarItem = styled(Box)({
  marginRight: 100,
  marginTop: 5,
});

export default function HeaderBar({ api }) {
  const router = useRouter();
  const [version, setVersion] = useState(null);

  const goHome = (event) => {
    event?.preventDefault();
    router.push('/');
  };

  useEffect(() => {
    const fetchVersion = async () => {
      if (version === null) {
        const versionMap = await api.fetchVersionMap();
        setVersion(versionMap.data?.version);
      }
    };

    fetchVersion();
  }, [api, version]);

  return (
    <>
      <CustomAppBar
        position='absolute'
        sx={{
          visibility: 'visible',
        }}
      >
        <CustomToolbar>
          <TitleBox>
            <InstanceLogo
              src='/static/base_logo.png'
              alt='Featureform'
              onClick={goHome}
              sx={{
                height: 50,
                marginTop: 1,
                marginLeft: -1,
                width: 50,
                flexGrow: 1,
                display: { xs: 'none', sm: 'block' },
              }}
            />
          </TitleBox>

          <ToolbarRight>
            <ToolbarItem>
              <SearchBar sx={{ margin: 'auto' }} homePage={false} />
            </ToolbarItem>
            <Box>
              {version && (
                <span
                  data-testid={'versionPropId'}
                >{`Version: ${version}`}</span>
              )}
            </Box>
          </ToolbarRight>
        </CustomToolbar>
      </CustomAppBar>
    </>
  );
}
