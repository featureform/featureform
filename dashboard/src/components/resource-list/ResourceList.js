import { createSelector } from '@reduxjs/toolkit';
import dynamic from 'next/dynamic';
import PropTypes from 'prop-types';
import React from 'react';
import { connect } from 'react-redux';
import ServerErrorPage from '../servererror/ServerErrorPage';
import { fetchResources, setCurrentType } from './ResourceSlice.js';
import { toggleTag } from './TagSlice.js';
import { setVariant } from './VariantSlice.js';
const ResourceListView = dynamic(() => import('./ResourceListView'), {
  ssr: false,
});

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
    const selector = selectFilteredResources(typeToUse);
    const item = state.resourceList[typeToUse];
    const activeVariants = state.selectedVariant[typeToUse];
    const activeTags = state.selectedTags[typeToUse];
    return {
      title: typeToUse,
      resources: selector(state),
      loading: item.loading,
      failed: item.failed,
      activeVariants: activeVariants,
      activeTags: activeTags,
    };
  };
};

const mapDispatchToProps = (_, initProps) => {
  return (dispatch) => ({
    setCurrentType: (type) => {
      dispatch(setCurrentType({ selectedType: type }));
    },
    fetch: (typeParam) => {
      dispatch(
        fetchResources({
          api: initProps.api,
          type: typeParam ? typeParam : initProps.type,
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
  });
};

class ResourceList extends React.Component {
  componentDidMount() {
    this.props.fetch();
    this.props.setCurrentType(this.props.type);
  }

  componentDidUpdate(prevProps) {
    if (prevProps.type !== this.props.type) {
      this.props.fetch(this.props.type);
      this.props.setCurrentType(this.props.type);
    }
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

export default connect(mapStateToProps, mapDispatchToProps)(ResourceList);
