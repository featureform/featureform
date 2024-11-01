// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import CircleOutlinedIcon from '@mui/icons-material/CircleOutlined';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import Rating from '@mui/material/Rating';
import Typography from '@mui/material/Typography';
import { styled, ThemeProvider } from '@mui/system';
import { DataGrid } from '@mui/x-data-grid';
import { useRouter } from 'next/router';
import PropTypes from 'prop-types';
import React, { useEffect } from 'react';
import { PrismAsyncLight as SyntaxHighlighter } from 'react-syntax-highlighter';
import json from 'react-syntax-highlighter/dist/cjs/languages/prism/json';
import python from 'react-syntax-highlighter/dist/cjs/languages/prism/python';
import sql from 'react-syntax-highlighter/dist/cjs/languages/prism/sql';
import { providerLogos } from '../../api/resources';
import Resource from '../../api/resources/Resource.js';
import { deepCopy } from '../../helper';
import theme from '../../styles/theme';

SyntaxHighlighter.registerLanguage('python', python);
SyntaxHighlighter.registerLanguage('sql', sql);
SyntaxHighlighter.registerLanguage('json', json);

const RootContainer = styled(Container)(({ theme }) => ({
  background: 'rgba(255, 255, 255, 0.5)',
  border: `2px solid ${theme.palette.border.main}`,
  borderRadius: 16,
  '& > *': {
    borderRadius: 16,
  },
}));

const NoDataPage = styled('div')(({ theme }) => ({
  '& > *': {
    padding: theme.spacing(1),
  },
}));

const UsageIcon = styled(Rating)({
  color: 'red',
});

const VariantTableContainer = styled('div')(({ theme }) => ({
  marginLeft: theme.spacing(4),
  marginRight: theme.spacing(4),
}));

const Tag = styled(Chip)(({ theme }) => ({
  margin: theme.spacing(0.1),
}));

const ProviderLogo = styled('img')({
  maxWidth: '6em',
  maxHeight: '2.5em',
});

const TableToolbar = styled('div')(({ theme }) => ({
  paddingTop: theme.spacing(3),
  paddingBottom: theme.spacing(1),
}));

export function filterMissingDefaults(row = {}) {
  const defaultVariant = row['default-variant'];
  if (row?.variants) {
    if (defaultVariant in row.variants) {
      return true;
    } else {
      console.warn(
        `The current default rowVariant (${defaultVariant}) is not present in the variants list:`,
        row.variants ? Object.keys(row.variants) : 'row.variants is undefined.'
      );
      return false;
    }
  }
  return true;
}

export const ResourceListView = ({
  title,
  resources,
  loading,
  failed,
  type,
  activeTags,
  activeVariants = {},
  setVariant,
  toggleTag,
}) => {
  const columnFormats = {
    default: [
      { field: 'name', headerName: 'Name', flex: 1 },
      { field: 'description', headerName: 'Description', flex: 2 },
    ],
    Model: [
      { field: 'name', headerName: 'Name', flex: 1 },
      { field: 'description', headerName: 'Description', flex: 2 },
    ],
    default_tags: [
      { field: 'name', headerName: 'Name', flex: 1 },
      { field: 'description', headerName: 'Description', flex: 2 },
      {
        field: 'tags',
        headerName: 'Tags',
        flex: 1,
        renderCell: (params) => (
          <TagList
            activeTags={activeTags}
            tags={params.row.tags}
            tagClass={Tag}
            toggleTag={toggleTag}
          />
        ),
      },
    ],
    Feature: [
      { field: 'name', headerName: 'Name', flex: 1 },
      { field: 'description', headerName: 'Description', flex: 2 },
      {
        field: 'data-type',
        headerName: 'Type',
        flex: 1,
      },
      {
        field: 'default-variant',
        headerName: 'Latest Variant',
        flex: 1,
      },
    ],
    Provider: [
      { field: 'name', headerName: 'Name', flex: 1 },
      { field: 'description', headerName: 'Description', flex: 2 },
      { field: 'type', headerName: 'Type', flex: 1 },
      {
        field: 'software',
        headerName: 'Software',
        flex: 1,
        renderCell: (params) => (
          <div className={ProviderLogo}>
            <img
              alt={params.row.software}
              src={
                providerLogos[params.row.software?.toUpperCase()] ??
                providerLogos['LOCALMODE']
              }
            />
          </div>
        ),
      },
    ],
    'Data Source': [
      { field: 'name', headerName: 'Name', flex: 1 },
      { field: 'description', headerName: 'Description', flex: 2 },
      { field: 'type', headerName: 'Type', flex: 1 },
    ],
    User: [{ field: 'name', headerName: 'Name', flex: 1 }],
    Entity: [
      { field: 'name', headerName: 'Name', flex: 1 },
      {
        field: 'description',
        headerName: 'Description',
        flex: 2,
      },
    ],
    Transformation: [
      { field: 'name', headerName: 'Name', flex: 1 },
      { field: 'description', headerName: 'Description', flex: 2 },
      {
        field: 'tags',
        headerName: 'Tags',
        flex: 1,
        renderCell: (params) => (
          <TagList
            activeTags={activeTags}
            tags={params.row.tags}
            tagClass={Tag}
            toggleTag={toggleTag}
          />
        ),
      },
      {
        field: 'default-variant',
        headerName: 'Latest Variant',
        flex: 1,
      },
    ],
  };

  const router = useRouter();
  const initialLoad = resources == null && !loading;
  const initRes = resources || [];

  useEffect(() => {
    // reset search text between type changes
    return;
  }, [type]);

  let mutableRes = deepCopy(initRes);
  mutableRes = mutableRes.filter(filterMissingDefaults);

  function detailRedirect(event, data) {
    event.stopPropagation();
    const base = Resource[type].urlPathResource(data.name);
    const defaultVariant = data?.['default-variant'];
    if (defaultVariant) {
      setVariant(type, data.name, defaultVariant);
      router.push(`${base}?variant=${defaultVariant}`);
    } else {
      router.push(base);
    }
  }

  function getPageSizeProp(listLength = 0) {
    let pageSize = 5;
    if (listLength > 10) {
      pageSize = 20;
    } else if (listLength > 5) {
      pageSize = 10;
    } else {
      pageSize = 5;
    }
    return pageSize;
  }

  let mainTableSize = getPageSizeProp(mutableRes?.length);

  return (
    <ThemeProvider theme={theme}>
      <RootContainer maxWidth='xl'>
        <Typography variant='h4'>
          <b>{Resource[type].typePlural}</b>
        </Typography>
        <DataGrid
          rows={mutableRes.map((row) => {
            let rowData = { id: row.name };
            if (!row.variants) {
              return { ...rowData, ...row };
            }
            let rowVariant = activeVariants[row.name] || row['default-variant'];
            return {
              ...rowData,
              ...row.variants[rowVariant],
              'default-variant': row['default-variant'],
              variants: Object.values(row.variants),
            };
          })}
          columns={
            Object.keys(columnFormats).includes(title)
              ? columnFormats[title]
              : columnFormats['default']
          }
          pageSize={mainTableSize}
          rowsPerPageOptions={[5, 10, 20]}
          loading={initialLoad || loading || failed}
          onRowClick={(params) => detailRedirect(params.event, params.row)}
          components={{
            Toolbar: TableToolbar,
            NoRowsOverlay: () => <NoDataMessage type={type} />,
          }}
        />
      </RootContainer>
    </ThemeProvider>
  );
};

export const TagList = ({ activeTags = {}, tags = [], toggleTag }) => (
  <Grid container direction='row' data-testid='tagContainerId'>
    {tags.map((tag, index) => (
      <Tag
        key={tag}
        data-testid={`${tag}-${index}`}
        color={activeTags[tag] ? 'secondary' : 'default'}
        onClick={(event) => {
          toggleTag(tag);
          event.stopPropagation();
        }}
        variant='outlined'
        label={tag}
      />
    ))}
  </Grid>
);

export const VariantTable = ({
  name,
  setVariant,
  type,
  row,
  pageSizeProp = 5,
}) => {
  const router = useRouter();

  function variantChangeRedirect(event, data) {
    event.stopPropagation();
    setVariant(type, name, data.variant);
    const base = Resource[type].urlPathResource(name);
    router.push(`${base}?variant=${data.variant}`);
  }

  const myVariants = row.variants.map((variant) => ({
    id: variant.variant,
    variant: variant.variant,
    description: variant.description,
  }));

  return (
    <VariantTableContainer>
      <ThemeProvider theme={theme}>
        <DataGrid
          rows={myVariants}
          columns={[
            { field: 'variant', headerName: 'Variants', flex: 1 },
            { field: 'description', headerName: 'Description', flex: 2 },
          ]}
          pageSize={pageSizeProp}
          rowsPerPageOptions={[5, 10, 20]}
          onRowClick={(params) =>
            variantChangeRedirect(params.event, params.row)
          }
          autoHeight
          components={{
            Toolbar: TableToolbar,
          }}
        />
      </ThemeProvider>
    </VariantTableContainer>
  );
};

const customIcons = {
  1: {
    icon: <CircleOutlinedIcon />,
    label: 'Unused',
  },
  2: {
    icon: <CircleOutlinedIcon />,
    label: 'Dissatisfied',
  },
  3: {
    icon: <CircleOutlinedIcon />,
    label: 'Neutral',
  },
  4: {
    icon: <CircleOutlinedIcon />,
    label: 'Satisfied',
  },
  5: {
    icon: <CircleOutlinedIcon />,
    label: 'Frequently used',
  },
};
function IconContainer(props) {
  const { value, ...other } = props;
  return <span {...other}>{customIcons[value].icon}</span>;
}

export const UsageTab = () => {
  return (
    <UsageIcon
      name='read-only'
      value={2}
      IconContainerComponent={IconContainer}
      readOnly
    />
  );
};

const NoDataMessage = ({ type }) => {
  function redirect() {
    const url = 'https://docs.featureform.com/getting-started/overview';
    const newWindow = window.open(url, '_blank', 'noopener,noreferrer');
    if (newWindow) newWindow.opener = null;
  }
  return (
    <Container data-testid='noDataContainerId'>
      <NoDataPage>
        <>
          <Typography variant='h4'>
            No {Resource[type].typePlural} Registered
          </Typography>
          <Typography variant='body1'>
            There are no visible {type.toLowerCase()}s in your organization.
          </Typography>
        </>
        <Typography variant='body1'>
          Check out our docs for step by step instructions to create one.
        </Typography>
        <Button variant='outlined' onClick={redirect}>
          FeatureForm Docs
        </Button>
      </NoDataPage>
    </Container>
  );
};

ResourceListView.propTypes = {
  title: PropTypes.string.isRequired,
  resources: PropTypes.array,
  loading: PropTypes.bool,
  failed: PropTypes.bool,
  activeVariants: PropTypes.object,
  setVariant: PropTypes.func,
};

export default ResourceListView;
