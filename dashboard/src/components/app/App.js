import Container from '@mui/material/Container';
import CssBaseline from '@mui/material/CssBaseline';
import { makeStyles, ThemeProvider } from '@mui/styles';
import React from 'react';
import theme from '../../styles/theme';
import BreadCrumbs from '../breadcrumbs/BreadCrumbs';
import TopBar from '../topbar/TopBar';

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
