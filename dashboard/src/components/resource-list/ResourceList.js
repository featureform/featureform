// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { createSelector } from '@reduxjs/toolkit';
import dynamic from 'next/dynamic';
import PropTypes from 'prop-types';
import React from 'react';
import { connect } from 'react-redux';
import ServerErrorPage from '../servererror/ServerErrorPage';
import DatasetTable from './DatasetTable';
import { EntityTable } from './EntityTable';
import { setCurrentFilter } from './FilterSlice';
import LabelTable from './LabelTable';
import ModelTable from './ModelTable';
import { setCurrentPage } from './PageSlice';
import ProviderTable from './ProviderTable/ProviderTable';
import { fetchResources, setCurrentType } from './ResourceSlice.js';
import { toggleTag } from './TagSlice.js';
import TrainingSetTable from './TrainingSetTable';
import { setVariant } from './VariantSlice.js';
const ResourceListView = dynamic(() => import('./ResourceListView'), {
  ssr: false,
});

export const DEFAULT_RESOURCE_LIST_SIZE = 10;

export const selectFilteredResources = (type) => {
  const selectResources = (state) => state.resourceList[type].resources;
  const selectTags = (state) => state.selectedTags[type];
  const activeVariants = (state) => state.selectedVariant[type];
  return createSelector(
    selectResources,
    selectTags,
    activeVariants,
    (resources, tags, variants) => {
      const isLoading = !resources;

      if (isLoading) {
        return null;
      }
      const numActiveTags = Object.keys(tags).length;
      const noTagsActive = numActiveTags === 0;

      if (noTagsActive) {
        return resources;
      }
      return resources.filter((resource) => {
        let activeVariantName = variants[resource.name]
          ? variants[resource.name]
          : resource['default-variant'];
        let activeResource = resource.variants[activeVariantName];
        const resTags = activeResource.tags || [];
        const numFound = resTags.filter((itemTag) => itemTag in tags).length;
        const hasAllTags = numFound === numActiveTags;
        return hasAllTags;
      });
    }
  );
};

const mapStateToProps = (_, ownProps) => {
  return (state) => {
    const selectedType = state.resourceList.selectedType;
    const typeToUse = selectedType ? selectedType : ownProps.type;
    const count = state.resourceList[selectedType]?.count;
    const selector = selectFilteredResources(typeToUse);
    const item = state.resourceList[typeToUse];
    const activeVariants = state.selectedVariant[typeToUse];
    const activeTags = state.selectedTags[typeToUse];
    const filter = state.currentFilter[typeToUse];
    return {
      title: typeToUse,
      resources: selector(state),
      loading: item.loading,
      failed: item.failed,
      activeVariants: activeVariants,
      activeTags: activeTags,
      pageSize: DEFAULT_RESOURCE_LIST_SIZE,
      currentPage: state.currentPage?.[typeToUse].currentPage,
      currentFilter: filter,
      count: count,
    };
  };
};

const mapDispatchToProps = (_, initProps) => {
  return (dispatch) => ({
    setCurrentType: (type) => {
      dispatch(setCurrentType({ selectedType: type }));
    },
    fetch: (typeParam, currentPage, currentFilter = {}) => {
      dispatch(
        fetchResources({
          api: initProps.api,
          type: typeParam ? typeParam : initProps.type,
          pageSize: DEFAULT_RESOURCE_LIST_SIZE,
          offset: currentPage,
          filter: currentFilter,
        })
      );
    },
    setVariant: (type, name, variant) => {
      dispatch(setVariant({ type, name, variant }));
    },
    toggleTag: (tag) => {
      const { type } = initProps;
      dispatch(toggleTag({ type, tag }));
    },
    setPage: (type, page) => {
      dispatch(setCurrentPage({ type: type, currentPage: page }));
    },
    setFilter: (type, filter) => {
      dispatch(setCurrentFilter({ type: type, currentFilter: filter }));
    },
  });
};

class ResourceList extends React.Component {
  componentDidMount() {
    this.props.setFilter(this.props.type, this.props.currentFilter);
    this.props.setCurrentType(this.props.type);
    this.props.fetch(this.props.type, this.props.currentPage, {});
  }

  componentDidUpdate(prevProps) {
    if (prevProps.type !== this.props.type) {
      this.props.setCurrentType(this.props.type);
      this.props.setFilter(this.props.type, {});
      this.props.setPage(this.props.type, 0);
      this.props.fetch(this.props.type, 0, {});
    } else if (prevProps.currentPage !== this.props.currentPage) {
      this.props.setFilter(this.props.type, this.props.currentFilter);
      this.props.fetch(
        this.props.type,
        this.props.currentPage,
        this.props.currentFilter
      );
    } else if (prevProps.currentFilter !== this.props.currentFilter) {
      this.props.fetch(
        this.props.type,
        this.props.currentPage,
        this.props.currentFilter
      );
    }
  }

  render() {
    const viewProps = { ...this.props };
    delete viewProps.api;
    delete viewProps.fetch;

    const errorPage = <ServerErrorPage />;
    const providerTable = <ProviderTable {...viewProps} />;
    const trainingSetTable = <TrainingSetTable {...viewProps} />;
    const entityTable = <EntityTable {...viewProps} />;
    const labelTable = <LabelTable {...viewProps} />;
    const modelTable = <ModelTable {...viewProps} />;
    const datasetTable = <DatasetTable {...viewProps} />;

    let body = null;
    const typeComponentMap = {
      Provider: providerTable,
      TrainingSet: trainingSetTable,
      Entity: entityTable,
      Label: labelTable,
      Model: modelTable,
      Source: datasetTable,
    };

    if (viewProps.failed) {
      body = errorPage;
    } else if (viewProps.type in typeComponentMap) {
      body = typeComponentMap[viewProps.type];
    } else {
      body = <ResourceListView {...viewProps} />;
    }

    return body;
  }
}

ResourceList.propTypes = {
  type: PropTypes.string.isRequired,
  api: PropTypes.object.isRequired,
  title: PropTypes.string,
  resources: PropTypes.array,
  loading: PropTypes.bool,
  failed: PropTypes.bool,
  pageSize: PropTypes.number,
  currentPage: PropTypes.number,
  currentFilter: PropTypes.object,
  count: PropTypes.number,
};

export default connect(mapStateToProps, mapDispatchToProps)(ResourceList);
