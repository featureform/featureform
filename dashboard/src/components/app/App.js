import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import CssBaseline from "@material-ui/core/CssBaseline";
import theme from "../../styles/theme";
import { ThemeProvider } from "@material-ui/core/styles";
import ResourcesAPI from "../../api/resources";
import HomePage from "../homepage/HomePage";
import DataPage from "../datapage/DataPage";
import TopBar from "../topbar/TopBar";
import EntityPage from "../entitypage/EntityPage";
import BreadCrumbs from "../breadcrumbs/BreadCrumbs";
import Container from "@material-ui/core/Container";
import SearchResults from "../searchresults/SearchResults";
import NotFoundPage from "../notfoundpage/NotFoundPage";
import ConnectionPage from "../connectionpage";

const apiHandle = new ResourcesAPI();
const useStyles = makeStyles((theme) => ({
  pageContainer: {
    paddingLeft: theme.spacing(8),
    paddingRight: theme.spacing(8),
  },
}));

export const App = ({ Component, pageProps }) => {
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
        <Component {...pageProps} />
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
