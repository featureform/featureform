import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import CssBaseline from "@material-ui/core/CssBaseline";
import theme from "styles/theme";
import { ThemeProvider } from "@material-ui/core/styles";
import { Route } from "react-router-dom";
import ResourcesAPI from "api/resources";
import HomePage from "../homepage/HomePage";
import DataPage from "../datapage/DataPage";
import { Switch } from "react-router-dom";
import TopBar from "../topbar/TopBar";
import EntityPage from "components/entitypage/EntityPage";
import BreadCrumbs from "components/breadcrumbs/BreadCrumbs";
import Container from "@material-ui/core/Container";
import SearchResults from "../searchresults/SearchResults";
import NotFoundPage from "components/notfoundpage/NotFoundPage";
import ConnectionPage from "components/connectionpage";

const apiHandle = new ResourcesAPI();
const useStyles = makeStyles((theme) => ({
  pageContainer: {
    paddingLeft: theme.spacing(8),
    paddingRight: theme.spacing(8),
  },
}));

export const App = ({ ...props }) => {
  const classes = useStyles();
  return (
    <ThemeWrapper>
      <TopBar className={classes.topbar} />
      <Container
        maxWidth="xl"
        className={classes.root}
        classes={{ maxWidthXl: classes.pageContainer }}
      >
        <BreadCrumbs />
        <Switch>
          <Route exact path="/">
            <HomePage />
          </Route>
          <Route exact path="/search">
            <SearchResults api={apiHandle} />
          </Route>
          <Route exact path="/connections">
            <ConnectionPage />
          </Route>
          <Route exact path="/:type">
            <DataPage />
          </Route>
          <Route path="/:type/:entity">
            <EntityPage api={apiHandle} />
          </Route>
          <Route component={NotFoundPage} />
        </Switch>
      </Container>
    </ThemeWrapper>
  );
};

export const views = {
  RESOURCE_LIST: "ResourceList",
  EMPTY: "Empty",
};

export const ThemeWrapper = ({ children }) => (
  <ThemeProvider theme={theme}>
    <CssBaseline />
    {children}
  </ThemeProvider>
);

export default App;
