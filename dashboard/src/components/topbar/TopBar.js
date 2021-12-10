import React, { useEffect } from "react";
import { makeStyles } from "@material-ui/core/styles";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import IconButton from "@material-ui/core/IconButton";
import AccountCircle from "@material-ui/icons/AccountCircle";
import Typography from "@material-ui/core/Typography";
import MenuItem from "@material-ui/core/MenuItem";
import Menu from "@material-ui/core/Menu";
import { useLocation } from "react-router-dom";
import SearchBar from "../search/SearchBar";
import Divider from "@material-ui/core/Divider";

const useStyles = makeStyles((theme) => ({
  root: {
    diplay: "flex",
    width: "100%",
  },

  appbar: {
    background: "transparent",
    boxShadow: "none",
    display: "flex",
    width: "100%",
    color: "black",
  },
  searchBar: {},
  toolbar: {
    width: "100%",
    display: "flex",
    justifyContent: "space-between",
  },
  title: {
    justifySelf: "flex-start",
    paddingLeft: theme.spacing(6),
  },
  instanceName: {
    userSelect: "none",
    opacity: "50%",
  },
  toolbarRight: {
    alignItems: "center",
    display: "flex",
    paddingRight: theme.spacing(4),
  },
  accountButton: {
    color: "black",
    position: "relative",
    fontSize: "3em",
    padding: "1em",
    width: "auto",
    justifySelf: "flex-end",
  },
}));

export default function TopBar() {
  let location = useLocation();
  const classes = useStyles();
  let auth = false;

  const instanceName = "PayForce";

  const [search, setSearch] = React.useState(false);
  const [anchorEl, setAnchorEl] = React.useState(null);
  const open = Boolean(anchorEl);

  useEffect(() => {
    if (location.pathname !== "/") {
      setSearch(true);
    } else {
      setSearch(false);
    }
  }, [location]);

  const handleMenu = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  return (
    <div className={classes.root}>
      <AppBar position="static" className={classes.appbar}>
        <Toolbar className={classes.toolbar}>
          <div className={classes.title}>
            <img
              src="/FeatureForm_Logo_Full_Black.svg"
              height={30}
              alt="Featureform"
              component="div"
              nowrap
              sx={{ flexGrow: 1, display: { xs: "none", sm: "block" } }}
            />
          </div>

          <div className={classes.toolbarRight}>
            {search && (
              <SearchBar className={classes.searchBar} homePage={false} />
            )}
            {search && (
              <Divider orientation="vertical" variant="middle" flexItem />
            )}
            <div className={classes.instanceName}>
              <Typography variant="h6">{instanceName}</Typography>
            </div>
            {auth && (
              <div>
                <IconButton
                  aria-label="account of current user"
                  aria-controls="menu-appbar"
                  aria-haspopup="true"
                  onClick={handleMenu}
                  color="inherit"
                  className={classes.accountButton}
                >
                  <AccountCircle />
                </IconButton>
                <Menu
                  id="menu-appbar"
                  anchorEl={anchorEl}
                  anchorOrigin={{
                    vertical: "top",
                    horizontal: "right",
                  }}
                  keepMounted
                  transformOrigin={{
                    vertical: "top",
                    horizontal: "right",
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
