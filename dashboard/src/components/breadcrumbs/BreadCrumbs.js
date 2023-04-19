import React from "react";
import { useRouter } from 'next/router';
import { makeStyles } from "@material-ui/core/styles";
import Link from 'next/link';
import Breadcrumbs from "@material-ui/core/Breadcrumbs";
import NavigateNextIcon from "@mui/icons-material/NavigateNext";

const useStyles = makeStyles((theme) => ({
  root: {
    margin: 5,
  },
  ol: {
    alignItems: "inherit",
  },
  breadcrumbs: {
    fontSize: 18,
  },
  separator: {
    marginLeft: "0.2em",
    marginRight: "0.2em",
    alignItems: "auto",
  },
}));

const BreadCrumbs = () => {
  const classes = useStyles();
  const { asPath } = useRouter();
  let path = asPath.split("/");
  path = path.slice(-1); //grab the query params
  const capitalize = (word) => {
    return word ? word[0].toUpperCase() + word.slice(1).toLowerCase() : '';
  };

  // console.log('path after slice', path);
  let params = new URLSearchParams(path[path.length - 1])
  // const pathBuilder = (accumulator, currentValue) =>
  //   accumulator + "/" + currentValue;
  return (
    <div className={classes.root}>
      <Breadcrumbs
        className={classes.breadcrumbs}
        style={{ margin: "0.25em" }}
        aria-label="breadcrumb"
        separator={<NavigateNextIcon fontSize="medium" />}
        classes={{
          separator: classes.separator,
          ol: classes.ol,
        }}
      >
        {[...params.entries()].length && <Link href="/">Home</Link>}
        {[...params.entries()].map((entry, index) => {
          if (entry[0] == 'type') {
            return <Link key={`link-${index}`} href={'/datapage/?type=' + entry[1]}>{capitalize(entry[1])}</Link>
          }
          else { return <span>{capitalize(entry[1])}</span> }
        }
        )}
      </Breadcrumbs>
      {/* {path.length > 0 ? (
        <Breadcrumbs
          className={classes.breadcrumbs}
          style={{ margin: "0.25em" }}
          aria-label="breadcrumb"
          separator={<NavigateNextIcon fontSize="medium" />}
          classes={{
            separator: classes.separator,
            ol: classes.ol,
          }}
        >
          <Link href="/">Home</Link>
          {path.map((ent, i) => (
            <Link
              key={`link-${i}`}
              href={"/" + path.slice(0, i + 1).reduce(pathBuilder)}
            >
              {i === path.length - 1 ? (
                <b>{capitalize(ent)}</b>
              ) : (
                capitalize(ent)
              )}
            </Link>
          ))}
        </Breadcrumbs>
      ) : (
        <div></div>
      )} */}
    </div>
  );
};

export default BreadCrumbs;
