import Container from '@material-ui/core/Container';
import CssBaseline from '@material-ui/core/CssBaseline';
import { makeStyles, ThemeProvider } from '@material-ui/core/styles';
import React from 'react';
import ResourcesAPI from '../../api/resources';
import theme from '../../styles/theme';
import BreadCrumbs from '../breadcrumbs/BreadCrumbs';
import TopBar from '../topbar/TopBar';

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
        maxWidth='xl'
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
  RESOURCE_LIST: 'ResourceList',
  EMPTY: 'Empty',
};

export const ThemeWrapper = ({ children }) => (
  <ThemeProvider theme={theme}>
    <CssBaseline />
    {children}
  </ThemeProvider>
);

export default App;
