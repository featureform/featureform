import React, { useEffect } from "react";
import { makeStyles } from "@material-ui/core/styles";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import AccountCircle from "@material-ui/icons/AccountCircle";
import MenuItem from "@material-ui/core/MenuItem";
import Menu from "@material-ui/core/Menu";
import Box from "@material-ui/core/Box";
import { useLocation } from "react-router-dom";
import SearchBar from "../search/SearchBar";

const useStyles = makeStyles((theme) => ({
  root: {
    //height: "00px",
    diplay: "flex",
    width: "100%",
    //justifyContent: "space-between",
  },

  menuButton: {
    //marginRight: theme.spacing(2),
  },
  appbar: {
    background: "transparent",
    boxShadow: "none",
    display: "flex",
    width: "100%",
    color: "black",
    //justifyContent: "space-between",
  },
  searchBar: {
    //alignSelf: "flex-end",
  },
  toolbar: {
    width: "100%",
    display: "flex",
    justifyContent: "space-between",
  },
  title: {
    justifySelf: "flex-start",
  },
  toolbarRight: {
    alignItems: "center",
    display: "flex",
  },
  accountButton: {
    color: "black",
    position: "relative",
    //marginLeft: theme.spacing(10),
    width: "auto",
    //justifyContent: "flex-end",
    //alignItems: "flex-end",
    justifySelf: "flex-end",
  },
}));

export default function TopBar() {
  let location = useLocation();
  const classes = useStyles();
  const auth = true;

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
              src="/Featureform_Logo_Full_Black.svg"
              height={30}
              //width={150}
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
        </Toolbar>
      </AppBar>
    </div>
  );
}
