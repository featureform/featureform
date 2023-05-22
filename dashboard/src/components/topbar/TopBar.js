import { Box } from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import { makeStyles } from '@material-ui/core/styles';
import Toolbar from '@material-ui/core/Toolbar';
import AccountCircle from '@material-ui/icons/AccountCircle';
import { useRouter } from 'next/router';
import React, { useEffect } from 'react';
import SearchBar from '../search/SearchBar';

const useStyles = makeStyles((theme) => ({
  root: {
    diplay: 'flex',
    width: '100%',
  },

  appbar: {
    position: 'fixed',
    top: '0',
    width: '100%',
    height: '70px',
    left: '0px',
    background: `linear-gradient(270deg, #6BF77A 0%, #7A14E5 37.22%)`,
    borderBottom: `1px solid #E5E5E5`,
  },
  instanceLogo: {
    height: '3em',
  },
  searchBar: {
    margin: 'auto',
  },
  toolbar: {
    width: '100%',
    display: 'flex',
    justifyContent: 'space-between',
  },
  title: {
    justifySelf: 'flex-start',
    paddingLeft: theme.spacing(1.5),
  },
  instanceName: {
    userSelect: 'none',
    opacity: '50%',
    padding: theme.spacing(1),
  },
  toolbarRight: {
    alignItems: 'center',
    display: 'flex',
    paddingRight: theme.spacing(4),
  },
  accountButton: {
    color: 'black',
    position: 'relative',
    fontSize: '3em',
    padding: '1em',
    width: 'auto',
    justifySelf: 'flex-end',
  },
}));

export default function TopBar({ api }) {
  let router = useRouter();
  const classes = useStyles();
  let auth = false;

  const [anchorEl, setAnchorEl] = React.useState(null);
  const [version, setVersion] = React.useState('');
  const open = Boolean(anchorEl);

  const handleMenu = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const goHome = (event) => {
    event?.preventDefault();
    router.push('/');
  };

  useEffect(async () => {
    if (!version) {
      const versionMap = await api.fetchVersionMap();
      setVersion(versionMap.data?.version);
    }
  }, [api]);

  return (
    <div className={classes.root}>
      <AppBar position='absolute' className={classes.appbar}>
        <Toolbar className={classes.toolbar}>
          <div className={classes.title}>
            <Box
              component={'img'}
              src='/static/FeatureForm_Logo_Full_White.svg'
              alt='Featureform'
              onClick={goHome}
              style={{
                cursor: 'pointer',
                nowrap: true,
              }}
              sx={{
                height: 30,
                flexGrow: 1,
                display: { xs: 'none', sm: 'block' },
              }}
            />
          </div>

          <div className={classes.toolbarRight}>
            <SearchBar className={classes.searchBar} homePage={false} />
            {auth && (
              <div>
                <IconButton
                  aria-label='account of current user'
                  aria-controls='menu-appbar'
                  aria-haspopup='true'
                  onClick={handleMenu}
                  color='inherit'
                  className={classes.accountButton}
                >
                  <AccountCircle />
                </IconButton>
                <Menu
                  id='menu-appbar'
                  anchorEl={anchorEl}
                  anchorOrigin={{
                    vertical: 'top',
                    horizontal: 'right',
                  }}
                  keepMounted
                  transformOrigin={{
                    vertical: 'top',
                    horizontal: 'right',
                  }}
                  open={open}
                  onClose={handleClose}
                >
                  <MenuItem onClick={handleClose}>Profile</MenuItem>
                  <MenuItem onClick={handleClose}>My account</MenuItem>
                </Menu>
              </div>
            )}
          </div>
          <span data-testid={'versionPropId'}>{version}</span>
        </Toolbar>
      </AppBar>
    </div>
  );
}
