import { ExpandLess, ExpandMore } from '@mui/icons-material';
import { Collapse, Icon } from '@mui/material';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
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
  const [open, setOpen] = React.useState(true);

  const handleCollapseClick = () => {
    setOpen(!open);
  };
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
              <ListItemButton
                key={index}
                onClick={(event) =>
                  handleMenuClick(event, resourceType.urlPath)
                }
              >
                <ListItemIcon>
                  <Icon>{resourceType.materialIcon}</Icon>
                </ListItemIcon>
                <ListItemText primary={resourceType.typePlural} />
              </ListItemButton>
            );
          })}
          <ListItemButton key={20} onClick={handleCollapseClick}>
            <ListItemIcon>
              <Icon>{'all_inbox'}</Icon>
            </ListItemIcon>
            <ListItemText primary={'Scheduling'} />
            {open ? <ExpandLess /> : <ExpandMore />}
          </ListItemButton>
        </List>
        <Collapse in={open} timeout='auto' unmountOnExit>
          <List component='div' disablePadding>
            <ListItemButton
              onClick={(event) => handleMenuClick(event, '/tasks')}
              sx={{ padding: '0 0 0 6em' }}
            >
              <ListItemText primary='Jobs' />
            </ListItemButton>
            <ListItemButton
              onClick={(event) => handleMenuClick(event, '/triggers')}
              sx={{ padding: '0 0 0 6em' }}
            >
              <ListItemText primary='Triggers' />
            </ListItemButton>
          </List>
        </Collapse>
      </Drawer>
      <Box
        component='main'
        sx={{
          flexGrow: 1,
          bgcolor: 'background.default',
          p: 2,
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
