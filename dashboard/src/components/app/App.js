import React from "react";
import { connect } from "react-redux";
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

const apiHandle = new ResourcesAPI();

export const App = ({ ...props }) => {
  return (
    <ThemeWrapper>
      {/* <Nav sections={sections}>{routes(sections)}</Nav> */}
      <TopBar />
      <Container maxWidth="xl">
        <BreadCrumbs />
        <Switch>
          <Route exact path="/">
            <HomePage />
          </Route>
          <Route exact path="/search">
            <SearchResults api={apiHandle} />
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

export const indexPath = (content) => {
  if (content.length === 0) {
    throw TypeError("Nav cannot be empty");
  }
  return content[0].path;
};

export function parseContentProps(sections) {
  return sections
    .flatMap((section) => section.items)
    .filter((item) => !item.external)
    .map(parseItem);
}

const parseItem = (item) => ({
  title: item.title,
  view: item.view || views.EMPTY,
  viewProps: item.viewProps || {},
  path: item.path,
});

export const ThemeWrapper = ({ children }) => (
  <ThemeProvider theme={theme}>
    <CssBaseline />
    {children}
  </ThemeProvider>
);

const mapStateToProps = (state) => ({
  sections: state.navSections,
});

export default connect(mapStateToProps)(App);
