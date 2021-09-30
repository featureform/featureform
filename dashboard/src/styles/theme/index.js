import blueGrey from "@material-ui/core/colors/blueGrey";
import { createTheme, responsiveFontSizes } from "@material-ui/core/styles";

export const GREEN = "#27ae60";
export const PURPLE = "#8e44ad";
export const FEATUREFORM_RED = "#f7195c";

const themeSpec = {
  palette: {
    primary: { main: FEATUREFORM_RED, contrastText: "#FFFFFF" },
    secondary: { main: PURPLE },
    text: {
      primary: blueGrey[900],
      secondary: blueGrey[700],
    },
  },
};

let theme = createTheme(themeSpec);
theme = responsiveFontSizes(theme);

export default theme;
