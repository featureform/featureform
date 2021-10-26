import blueGrey from "@material-ui/core/colors/blueGrey";
import { createTheme, responsiveFontSizes } from "@material-ui/core/styles";

export const GREEN = "#27ae60";
export const PURPLE = "#8e44ad";
export const FEATUREFORM_RED = "#f7195c";

const themeSpec = {
  overrides: {
    MuiCssBaseline: {
      "@global": {
        body: {
          background:
            "radial-gradient(circle farthest-side at 100% 55%, #E4D0FA 0%, transparent 40%), radial-gradient(circle farthest-side at 10% 140%, #F7195C 0%, transparent 40%);",
          backgroundRepeat: "no-repeat",
          backgroundAttachment: "fixed",
        },
      },
    },
  },
  palette: {
    background: {
      default: "#FFFFFF",
    },
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
