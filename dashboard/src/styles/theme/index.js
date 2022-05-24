import blueGrey from "@material-ui/core/colors/blueGrey";
import { createTheme, responsiveFontSizes } from "@material-ui/core/styles";

export const GREEN = "#27ae60";
export const FEATUREFORM_PURPLE = "#7A14E5";
export const FEATUREFORM_DARK_PURPLE = "#3D0A73";
export const FEATUREFORM_LIGHT_PURPLE = "#6a32a6";
export const FEATUREFORM_RED = "#f7195c";
export const FEATUREFORM_LIGHT_RED = "#F9477D";
export const FEATUREFORM_LIGHT_GRAY = "#F5F6F7";
export const FEATUREFORM_DARK_GRAY = "#CDD1D9";
export const FEATUREFORM_LIGHT_BLACK = "#061B3F";
export const FEATUREFORM_RED_BROWN = "#7C0D2E";
const themeSpec = {
  overrides: {
    MuiCssBaseline: {
      "@global": {
        body: {
          background:
            "radial-gradient(circle farthest-side, #E4D0FA 0%, rgba(255, 0, 0, 0) 40%) top left/200% 100%, radial-gradient(circle farthest-side, #FCA3BE 0%, rgba(255, 0, 0, 0) 40%) top right/180% 150%;",
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
    secondary: { main: FEATUREFORM_RED },
    error: { main: FEATUREFORM_RED },
    border: { main: FEATUREFORM_LIGHT_GRAY, alternate: FEATUREFORM_DARK_GRAY },
    text: {
      primary: blueGrey[900],
      secondary: blueGrey[700],
    },
    ordinalColors: {
      0: FEATUREFORM_DARK_PURPLE,
      1: FEATUREFORM_RED,
      2: FEATUREFORM_LIGHT_PURPLE,
      3: FEATUREFORM_LIGHT_RED,
    },
    hoverColors: {
      0: "#CBC3E3",
      1: "#FFCCCB",
    },
  },
  typography: {
    fontFamily: `"Matter", "Lato", "Helvetica", sans-serif`,
    fontSize: 14,
    fontWeightLight: 450,
    fontWeightRegular: 550,
    fontWeightMedium: 650,
  },
};

let theme = createTheme(themeSpec);
theme = responsiveFontSizes(theme);

export default theme;
