import React from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import { createSelector } from "@reduxjs/toolkit";
import { fetchResources } from "./ResourceSlice.js";
import ResourceListView from "./ResourceListView.js";
import { setVersion } from "./VersionSlice.js";
import { toggleTag } from "./TagSlice.js";
import ServerErrorPage from "../servererror/ServerErrorPage";

export const makeSelectFilteredResources = (type) => {
  const selectResources = (state) => state.resourceList[type].resources;
  const selectTags = (state) => state.selectedTags[type];
  const activeVersions = (state) => state.selectedVersion[type];
  return createSelector(
    selectResources,
    selectTags,
    activeVersions,
    (resources, tags, versions) => {
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
        let activeVersionName = versions[resource.name]
          ? versions[resource.name]
          : resource["default-version"];
        let activeResource = resource.versions[activeVersionName];
        const resTags = activeResource.tags || [];
        const numFound = resTags.filter((itemTag) => itemTag in tags).length;
        const hasAllTags = numFound === numActiveTags;
        return hasAllTags;
      });
    }
  );
};

const makeMapStateToProps = (initState, initProps) => {
  const type = initProps.type;
  return (state) => {
    const selector = makeSelectFilteredResources(type);
    const item = state.resourceList[type];
    const activeVersions = state.selectedVersion[type];
    const activeTags = state.selectedTags[type];
    return {
      title: type,
      resources: selector(state),
      loading: item.loading,
      failed: item.failed,
      activeVersions: activeVersions,
      activeTags: activeTags,
    };
  };
};

const makeMapDispatchToProps = (ignore, initProps) => {
  return (dispatch) => ({
    fetch: () => {
      const { type, api } = initProps;
      dispatch(fetchResources({ api, type }));
    },
    setVersion: (name, version) => {
      const { type } = initProps;
      dispatch(setVersion({ type, name, version }));
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
    // Only pass down props required for the view.
    // sends down props resources, loading, and failed
    const { api, fetch, type, ...viewProps } = this.props;
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
