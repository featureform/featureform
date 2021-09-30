import React from "react";
import { connect } from "react-redux";
import ResourcesAPI from "../../api/resources";
import ResourceList from "../resource-list/ResourceList";
import { useParams } from "react-router-dom";
import NotFoundPage from "../notfoundpage/NotFoundPage";

const apiHandle = new ResourcesAPI();

const DataPage = ({ ...props }) => {
  const { type } = useParams();
  let currentSection = props.sections[0].items.find(
    (section) => section.path === "/" + type
  );

  return (
    <div>
      {currentSection ? (
        <ResourceList api={apiHandle} {...currentSection.viewProps} />
      ) : (
        <NotFoundPage />
      )}
    </div>
  );
};

const mapStateToProps = (state) => ({
  sections: state.navSections,
});

export default connect(mapStateToProps)(DataPage);
