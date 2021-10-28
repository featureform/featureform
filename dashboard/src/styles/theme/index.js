import blueGrey from "@material-ui/core/colors/blueGrey";
import { createTheme, responsiveFontSizes } from "@material-ui/core/styles";

export const GREEN = "#27ae60";
export const FEATUREFORM_PURPLE = "#7A14E5";
export const FEATUREFORM_RED = "#f7195c";

const themeSpec = {
  overrides: {
    MuiCssBaseline: {
      "@global": {
        body: {
          background:
            "radial-gradient(circle farthest-side at 100% 55%, #E4D0FA 0%, transparent 40%), radial-gradient(circle farthest-side at 0% 120%, #FCA3BE 0%, transparent 40%);",
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
    secondary: { main: FEATUREFORM_PURPLE },
    text: {
      primary: blueGrey[900],
      secondary: blueGrey[700],
    },
  },
  typography: {
    fontFamily: `"Matter", "Helvetica", "Arial", sans-serif`,
    fontSize: 14,
    fontWeightLight: 450,
    fontWeightRegular: 550,
    fontWeightMedium: 650,
  },
};

let theme = createTheme(themeSpec);
theme = responsiveFontSizes(theme);

export default theme;
