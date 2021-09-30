import React from "react";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";
import IconButton from "@material-ui/core/IconButton";
import MenuIcon from "@material-ui/icons/Menu";
import { makeStyles } from "@material-ui/core/styles";
import Drawer from "@material-ui/core/Drawer";
import Divider from "@material-ui/core/Divider";
import Box from "@material-ui/core/Box";
import { PURPLE } from "styles/theme";
import Hidden from "@material-ui/core/Hidden";
import Grid from "@material-ui/core/Grid";
import List from "@material-ui/core/List";
import ListSubheader from "@material-ui/core/ListSubheader";
import ListItem from "@material-ui/core/ListItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import ListItemText from "@material-ui/core/ListItemText";
import { BrowserRouter as Router, NavLink } from "react-router-dom";

import Icon from "@material-ui/core/Icon";

const drawerWidth = 240;

const useStyles = makeStyles((theme) => ({
  root: {
    height: "100vh",
    overflow: "hidden",
  },
  appBar: {
    [theme.breakpoints.up("sm")]: {
      position: "fixed",
      zIndex: theme.zIndex.drawer + 1,
      top: 0,
      left: 0,
      right: 0,
      overflow: "hidden",
    },
  },

  drawerContainer: {
    [theme.breakpoints.up("sm")]: {
      width: drawerWidth,
      flexShrink: 0,
    },
  },
  drawer: {
    [theme.breakpoints.up("sm")]: {
      width: drawerWidth,
      flexShrink: 0,
    },
  },
  drawerPaper: {
    width: drawerWidth,
  },
  menuButton: {
    marginRight: theme.spacing(2),
    [theme.breakpoints.up("sm")]: {
      display: "none",
    },
  },
  toolbar: theme.mixins.toolbar,
  container: {
    height: "100vh",
    width: "100%",
    overflow: "hidden",
  },
  content: {
    height: "100vh",
    width: "100%",
    overflow: "hidden",
    flexWrap: "noWrap",
  },
  navlink_active: {
    color: PURPLE,
    // Prior to this, elements inside of <a> were overriding color, this
    // makes it more specific since we can't use !important.
    "& span": {
      color: PURPLE,
    },
  },
  main: {
    flexGrow: 1,
    backgroundColor: theme.palette.background.default,
    overflowY: "auto",
    "&::-webkit-scrollbar-track": {
      backgroundColor: "#FFFFFF",
    },

    "&::-webkit-scrollbar": {
      width: "12px",
      backgroundColor: "#FFFFFF",
    },

    "&::-webkit-scrollbar-thumb": {
      backgroundColor: "rgba(0, 0, 0, 0.2)",
    },
    "&::-webkit-scrollbar-button": {
      display: "none",
      width: 0,
      height: 0,
    },
  },
}));

export const Nav = ({ children, sections }) => {
  const classes = useStyles();
  const [mobileOpen, setMobileOpen] = React.useState(false);
  const handleDrawerToggle = () => {
    setMobileOpen(!mobileOpen);
  };
  return (
    <Router>
      <Box display="flex" component="div" className={classes.root}>
        <NavDrawer
          classes={classes}
          sections={sections}
          handleDrawerToggle={handleDrawerToggle}
          mobileOpen={mobileOpen}
        />
        <Box className={classes.container}>
          <TopBar
            classes={classes}
            handleDrawerToggle={handleDrawerToggle}
            mobileOpen={mobileOpen}
          />
          <BelowTopBar classes={classes}>
            <Box component="main" className={classes.main}>
              {children}
            </Box>
          </BelowTopBar>
        </Box>
      </Box>
    </Router>
  );
};

export const TopBar = ({ classes, handleDrawerToggle }) => {
  return (
    <AppBar className={classes.appBar}>
      <Toolbar>
        <IconButton
          color="inherit"
          aria-label="open drawer"
          edge="start"
          onClick={handleDrawerToggle}
          className={classes.menuButton}
        >
          <MenuIcon />
        </IconButton>
        <Typography component="span" variant="h5">
          FeatureForm
        </Typography>
      </Toolbar>
    </AppBar>
  );
};

const BelowTopBar = ({ children, classes }) => (
  <Grid container direction="column" className={classes.content}>
    {/* Adds a div the size of TopBar to shift everything under it */}
    <div className={classes.toolbar} />
    {children}
  </Grid>
);

const NavDrawer = ({ classes, sections, mobileOpen, handleDrawerToggle }) => (
  <nav className={classes.drawerContainer}>
    <Hidden smUp implementation="css">
      <Drawer
        className={classes.drawer}
        variant="temporary"
        anchor="left"
        open={mobileOpen}
        onClose={handleDrawerToggle}
        classes={{
          paper: classes.drawerPaper,
        }}
        anchor="left"
      >
        <div className={classes.toolbar} />
        <DrawerLists classes={classes} sections={sections} />
      </Drawer>
    </Hidden>
    <Hidden xsDown implementation="css">
      <Drawer
        className={classes.drawer}
        variant="permanent"
        classes={{
          paper: classes.drawerPaper,
        }}
        anchor="left"
      >
        <div className={classes.toolbar} />
        <DrawerLists classes={classes} sections={sections} />
      </Drawer>
    </Hidden>
  </nav>
);

export const DrawerLists = ({ classes, sections }) =>
  sections
    .map((sectionProps) => (
      <DrawerList key={sectionProps.name} classes={classes} {...sectionProps} />
    ))
    // Add a divider before each section
    .flatMap((section, idx) => [<Divider key={idx} />, section]);

export const DrawerList = ({ classes, name, items }) => (
  <List>
    <ListSubheader>{name}</ListSubheader>
    {items.map(({ title, icon, path, external }) => (
      <DrawerListLink
        key={title}
        path={path}
        classes={classes}
        external={external}
      >
        <ListItem button>
          <ListItemIcon>
            {/* Prior to overflow being set to visible, fa-sitemap was being
              cut-off since its slightly larger than a typical icon. */}
            <Icon style={{ overflow: "visible" }} className={`fa fa-${icon}`} />
          </ListItemIcon>
          <ListItemText primary={title} />
        </ListItem>
      </DrawerListLink>
    ))}
  </List>
);

export function DrawerListLink({ classes, path, external, children }) {
  if (external) {
    return (
      // _blank opens a new tab and noopender noreferrer blocks a known security
      // issue. Read more here: https://mathiasbynens.github.io/rel-noopener/
      <a target="_blank" rel="noopener noreferrer" href={path}>
        {children}
      </a>
    );
  } else {
    return (
      <NavLink to={path} activeClassName={classes.navlink_active}>
        {children}
      </NavLink>
    );
  }
}

export default Nav;
