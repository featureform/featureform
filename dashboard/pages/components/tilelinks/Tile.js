import React, { useState, useEffect } from "react";
import Icon from "@material-ui/core/Icon";
import { makeStyles } from "@material-ui/core/styles";
import { useRouter } from "next/router";
import Button from "@material-ui/core/Button";
import Box from "@material-ui/core/Box";
import Typography from "@material-ui/core/Typography";
import Resource from "../../api/resources/Resource.js";

const useStyles = makeStyles((theme, id) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
    height: "100%",
  },
  card: {
    padding: theme.spacing(3),
    width: "100%",
    height: "100%",
    border: (id) => `2px solid ${theme.palette.ordinalColors[id % 4]}`,
    borderRadius: 16,
    background: "white",
    minWidth: "12em",
    textTransform: "none",
    "&:hover": {
      backgroundColor: (id) => `${theme.palette.hoverColors[id % 2]}`,
      color: `${theme.palette.ordinalColors[id % 4]}`,
    },
  },

  media: {
    height: 200,
  },

  button: {
    width: "100%",
    height: "100%",
  },
  buttonDisabled: {
    padding: theme.spacing(3),
    opacity: 0.3,
    width: "100%",
    height: "100%",
    border: (id) => `2px solid ${theme.palette.ordinalColors[id % 4]}`,
    borderRadius: 16,
    background: "white",
    textTransform: "none",
    minWidth: "12em",
  },
  icon: {
    marginRight: theme.spacing(2),
    color: (id) => theme.palette.ordinalColors[id % 4],
    fontSize: "2em",
  },
  title: {
    fontSize: "2em",
    color: (id) => theme.palette.ordinalColors[id % 4],
  },

  tileContent: {
    display: "flex",
    justifyContent: "space-around",
  },
}));

function ClientOnly({ children, ...delegated }) {
  const [hasMounted, setHasMounted] = useState(false);
  useEffect(() => {
    setHasMounted(true);
  }, []);
  if (!hasMounted) {
    return null;
  }
  return (
    <div {...delegated}>
      {children}
    </div>
  );
}

const Tile = ({ detail, id }) => {
  let router = useRouter();
  let resourceType = Resource[detail.type];
  const disabled = detail.disabled;
  const classes = useStyles(id);

  let buttonClass = classes.card;

  if (disabled) {
    buttonClass = classes.buttonDisabled;
  }

  const handleClick = (event) => {
    router.push(resourceType.urlPath);
  };

  return (
    <ClientOnly>
      <Box sx={{ boxShadow: 3 }}>
        <div className={classes.root}>
          <Button
            className={buttonClass}
            onClick={handleClick}
            disabled={disabled}
            focusRipple={true}
            variant="contained"
          >
            <div className={classes.tileContent}>
              <div>
                <Icon className={classes.icon}>{resourceType.materialIcon}</Icon>
              </div>

              <div>
                <Typography className={classes.title} variant="h5">
                  <b>{resourceType.typePlural}</b>
                </Typography>
              </div>
            </div>
          </Button>
        </div>
      </Box>
    </ClientOnly>
  );
};

export default Tile;
