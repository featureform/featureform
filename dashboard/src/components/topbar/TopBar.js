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
    background: 'transparent',
    boxShadow: 'none',
    display: 'flex',
    width: '100%',
    color: 'black',
  },
  instanceLogo: {
    height: '3em',
  },
  searchBar: {},
  toolbar: {
    width: '100%',
    display: 'flex',
    justifyContent: 'space-between',
  },
  title: {
    justifySelf: 'flex-start',
    paddingLeft: theme.spacing(6),
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

export default function TopBar() {
  let router = useRouter();
  const classes = useStyles();
  let auth = false;

  const [search, setSearch] = React.useState(false);
  const [anchorEl, setAnchorEl] = React.useState(null);
  const open = Boolean(anchorEl);

  useEffect(() => {
    if (router.pathname !== '/') {
      setSearch(true);
    } else {
      setSearch(false);
    }
  }, [router]);

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

  return (
    <div className={classes.root}>
      <AppBar position='static' className={classes.appbar}>
        <Toolbar className={classes.toolbar}>
          <div className={classes.title}>
            <Box
              component={'img'}
              src='/static/FeatureForm_Logo_Full_Black.svg'
              alt='Featureform'
              onClick={goHome}
              style={{ cursor: 'pointer', nowrap: true }}
              sx={{
                height: 30,
                flexGrow: 1,
                display: { xs: 'none', sm: 'block' },
              }}
            />
          </div>

          <div className={classes.toolbarRight}>
            {search && (
              <SearchBar className={classes.searchBar} homePage={false} />
            )}
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
        </Toolbar>
      </AppBar>
    </div>
  );
}
