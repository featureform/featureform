import { createSelector } from '@reduxjs/toolkit';
import PropTypes from 'prop-types';
import React from 'react';
import { connect } from 'react-redux';
import ServerErrorPage from '../servererror/ServerErrorPage';
import ResourceListView from './ResourceListView.js';
import { fetchResources } from './ResourceSlice.js';
import { toggleTag } from './TagSlice.js';
import { setVariant } from './VariantSlice.js';

export const makeSelectFilteredResources = (type) => {
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

const makeMapStateToProps = (_, initProps) => {
  const type = initProps.type;
  return (state) => {
    const selector = makeSelectFilteredResources(type);
    const item = state.resourceList[type];
    const activeVariants = state.selectedVariant[type];
    const activeTags = state.selectedTags[type];
    return {
      title: type,
      resources: selector(state),
      loading: item.loading,
      failed: item.failed,
      activeVariants: activeVariants,
      activeTags: activeTags,
    };
  };
};

const makeMapDispatchToProps = (_, initProps) => {
  return (dispatch) => ({
    fetch: () => {
      const { type, api } = initProps;
      dispatch(fetchResources({ api, type }));
    },
    setVariant: (type, name, variant) => {
      dispatch(setVariant({ type, name, variant }));
    },
    toggleTag: (tag) => {
      const { type } = initProps;
      dispatch(toggleTag({ type, tag }));
    },
  });
};

class ResourceList extends React.Component {
  componentDidMount() {
    this.props.fetch();
  }

  render() {
    const viewProps = { ...this.props };
    delete viewProps.api;
    delete viewProps.fetch;
    return viewProps.failed ? (
      <ServerErrorPage />
    ) : (
      <ResourceListView {...viewProps} />
    );
  }
}

ResourceList.propTypes = {
  type: PropTypes.string.isRequired,
  api: PropTypes.object.isRequired,
  title: PropTypes.string,
  resources: PropTypes.array,
  loading: PropTypes.bool,
  failed: PropTypes.bool,
};

export default connect(
  makeMapStateToProps,
  makeMapDispatchToProps
)(ResourceList);
