import { Icon } from '@mui/material';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import { useRouter } from 'next/router';
import React from 'react';
import { connect } from 'react-redux';
import Resource from '../../api/resources/Resource';

const drawerWidth = 250;
const drawerTop = 70;

function SideNav({ sections, children }) {
  let router = useRouter();
  function handleMenuClick(event, urlPath) {
    event.preventDefault();
    router.push(urlPath);
  }

  return (
    <Box sx={{ display: 'flex' }}>
      <CssBaseline />
      <Drawer
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            boxSizing: 'border-box',
            top: `${drawerTop}px`,
          },
        }}
        variant='permanent'
        anchor='left'
      >
        <List>
          {sections?.['features'].map((currentSection, index) => {
            let resourceType = Resource[currentSection.type];
            return (
              <ListItem key={index}>
                <ListItemButton
                  onClick={(event) =>
                    handleMenuClick(event, resourceType.urlPath)
                  }
                >
                  <ListItemIcon style={{ padding: '0 0 0 1.5em !important' }}>
                    <Icon>{resourceType.materialIcon}</Icon>
                  </ListItemIcon>
                  <ListItemText primary={resourceType.typePlural} />
                </ListItemButton>
              </ListItem>
            );
          })}
        </List>
      </Drawer>
      <Box
        component='main'
        sx={{
          top: '100px',
          left: '500px',
          flexGrow: 1,
          bgcolor: 'background.default',
          p: 3,
        }}
      >
        {children}
      </Box>
    </Box>
  );
}

const mapStateToProps = (state) => ({
  sections: state.homePageSections,
});

export default connect(mapStateToProps)(SideNav);
